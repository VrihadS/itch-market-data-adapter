// MarketDataServer + MarketDataClient + BookManager end to end over the in-process transport,
// with a fake clock. Covers subscription handling, delivery, sharing, limits and failure modes.
#include <memory>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include "check.h"
#include "itch_builders.h"
#include "itch/messaging/inproc_transport.hpp"
#include "itch/messaging/market_data_client.hpp"
#include "itch/messaging/market_data_server.hpp"

using namespace feed_handler;
using namespace feed_handler::messaging;
using namespace itchtest;

static uint64_t g_now = 1'000'000'000ull;
static uint64_t fake_clock() { return g_now; }

struct Rec : IMarketDataHandler {
  std::vector<std::string> log;            // event order: "ack", "l2s", "l2", "trade"
  std::vector<AckView> acks; std::vector<L2View> l2; std::vector<TradeView> trades;
  int gaps = 0, timeouts = 0;
  void on_ack(const AckView &a) override { log.push_back("ack"); acks.push_back(a); }
  void on_l2(const L2View &v) override { log.push_back(v.snapshot ? "l2s" : "l2"); l2.push_back(v); }
  void on_trade(const TradeView &t) override { log.push_back("trade"); trades.push_back(t); }
  void on_gap(uint16_t, StreamKind, uint64_t, uint64_t) override { ++gaps; }
  void on_server_timeout() override { ++timeouts; }
  void clear() { log.clear(); acks.clear(); l2.clear(); trades.clear(); }
};

struct Rig {
  std::unique_ptr<MarketDataClient> c; Rec rec;
};

struct World {
  InprocTransport tp; BookManager books{1 << 12}; NasdaqItchDecoder dec; std::unique_ptr<MarketDataServer> server;
  std::vector<Rig *> rigs;

  explicit World(ServerConfig cfg = {}) {
    cfg.control_udp_channel = "aeron:udp?endpoint=10.0.0.1:40100";
    cfg.now_ns = fake_clock;
    server = std::make_unique<MarketDataServer>(tp, books, cfg);
    CHECK(server->start());
  }
  std::unique_ptr<Rig> client(ClientConfig cc) {
    cc.now_ns = fake_clock;
    auto r = std::make_unique<Rig>();
    r->c = std::make_unique<MarketDataClient>(tp, cc);
    CHECK(r->c->connect());
    rigs.push_back(r.get());
    return r;
  }
  void feed(const Bytes &b) {
    size_t used = 0;
    CHECK(dec.DecodeMessage(reinterpret_cast<const char *>(b.data()), b.size(), used, books));
  }
  void pump(int rounds = 3) {
    for (int i = 0; i < rounds; ++i) {
      g_now += 100'000'000ull;
      server->poll_control();
      for (Rig *r : rigs) r->c->poll(r->rec);
    }
  }
};

static ClientConfig ipc_client() { ClientConfig c; c.server_host = "ipc"; return c; }
static ClientConfig udp_client(const char *reply_host, uint16_t port = 40200) {
  ClientConfig c; c.server_host = "10.0.0.1"; c.reply_host = reply_host; c.reply_port = port; return c; }

int main() {
  spdlog::set_level(spdlog::level::off);
  constexpr uint8_t L2 = kFeatureL2Book, TNS = kFeatureTimeAndSales;

  World w;
  w.feed(MsgR(1, "AAPL")); w.feed(MsgR(2, "MSFT"));
  w.feed(MsgA(1, 1, 'B', 10, "AAPL", 1000000)); w.feed(MsgA(1, 2, 'B', 20, "AAPL", 990000));
  w.feed(MsgA(1, 3, 'B', 30, "AAPL", 980000)); w.feed(MsgA(1, 4, 'S', 5, "AAPL", 1010000));
  w.feed(WithTs(MsgA(2, 5, 'B', 7, "MSFT", 3000000), 555));

  // ── 1. same-machine client (IPC): ack first, then an L2 snapshot ───────────
  auto a = w.client(ipc_client());
  CHECK(a->c->subscribe({"AAPL", "NOPE"}, L2 | TNS, 2) == 1);
  w.pump();
  CHECK((a->rec.log == std::vector<std::string>{"ack", "l2s"}));
  CHECK(a->rec.acks[0].status == Status::Ok && a->rec.acks[0].result_count == 2);
  CHECK(a->rec.acks[0].results[0].locate == 1 && a->rec.acks[0].results[0].status == (uint8_t)Status::Ok);
  CHECK(a->rec.acks[0].results[1].status == (uint8_t)Status::UnknownSymbol);
  { const L2View &s = a->rec.l2[0];
    CHECK(s.snapshot && s.locate == 1 && s.sequence == 1 && s.bid_count == 2 && s.ask_count == 1);
    CHECK(s.bids[0].price == 1000000 && s.bids[0].shares == 10 && s.bids[1].price == 990000 && s.bids[1].shares == 20);
    CHECK(s.asks[0].price == 1010000 && s.asks[0].shares == 5 && s.asks[0].orders == 1); }
  CHECK(w.books.interest(1) == (L2 | TNS) && w.books.interest_depth(1) == 2 && w.books.interest(2) == 0);
  a->rec.clear();

  // ── 2. updates: only changes inside the requested depth are sent ───────────
  w.feed(MsgA(1, 6, 'B', 15, "AAPL", 995000));   // rank 1  -> inside top 2
  w.feed(MsgA(1, 7, 'B', 15, "AAPL", 985000));   // rank 3  -> outside
  w.feed(MsgA(2, 8, 'B', 1, "MSFT", 3000100));   // other instrument
  w.pump();
  CHECK(a->rec.l2.size() == 1 && !a->rec.l2[0].snapshot && a->rec.l2[0].sequence == 2);
  CHECK(a->rec.l2[0].bid_count == 2 && a->rec.l2[0].bids[1].price == 995000 && a->rec.l2[0].bids[1].shares == 15);
  a->rec.clear();

  // ── 3. time & sales: the print comes before the book change ───────────────
  w.feed(WithTs(MsgE(1, 1, 4), 777));
  w.pump();
  CHECK((a->rec.log == std::vector<std::string>{"trade", "l2"}));
  CHECK(a->rec.trades[0].kind == 'E' && a->rec.trades[0].side == 'B' && a->rec.trades[0].price == 1000000);
  CHECK(a->rec.trades[0].shares == 4 && a->rec.trades[0].sequence == 1 && a->rec.trades[0].exchange_ts_ns == 777);
  CHECK(a->rec.l2[0].sequence == 3 && a->rec.l2[0].bids[0].shares == 6 && a->rec.l2[0].exchange_ts_ns == 777);
  CHECK(a->rec.gaps == 0);
  a->rec.clear();

  // ── 4. a remote (UDP) client with a different depth shares the instrument ──
  auto b = w.client(udp_client("10.0.0.2"));
  CHECK(b->c->subscribe({"AAPL"}, L2, 1) != 0);
  CHECK(b->c->subscribe({"MSFT"}, TNS) != 0);
  w.pump();
  CHECK(b->rec.acks.size() == 2 && b->rec.acks[0].status == Status::Ok && b->rec.l2.size() == 1);
  CHECK(b->rec.l2[0].snapshot && b->rec.l2[0].bid_count == 1 && b->rec.l2[0].ask_count == 1 && b->rec.l2[0].sequence == 1);
  CHECK(w.books.interest_depth(1) == 2);                      // union: max(2, 1)
  w.feed(MsgA(1, 9, 'B', 3, "AAPL", 1000500));                 // new best bid -> both see it
  w.pump();
  CHECK(a->rec.l2.size() == 1 && a->rec.l2[0].bid_count == 2 && a->rec.l2[0].bids[0].price == 1000500);
  CHECK(b->rec.l2.size() == 2 && b->rec.l2[1].bid_count == 1 && b->rec.l2[1].bids[0].price == 1000500 && b->rec.l2[1].sequence == 2);
  w.feed(MsgP(2, 'S', 9, "MSFT", 3000050, 31)); w.pump();      // MSFT print: only b asked for MSFT
  CHECK(b->rec.trades.size() == 1 && b->rec.trades[0].kind == 'P' && b->rec.trades[0].side == 'S');
  CHECK(a->rec.trades.empty() && a->rec.gaps == 0 && b->rec.gaps == 0);
  a->rec.clear(); b->rec.clear();

  // ── 5. re-subscribe replaces the request, refreshes the snapshot, keeps sequence ──
  CHECK(a->c->subscribe({"AAPL"}, L2, 1) != 0); w.pump();
  CHECK(a->rec.l2.size() == 1 && a->rec.l2[0].snapshot && a->rec.l2[0].bid_count == 1);
  CHECK(a->rec.l2[0].sequence == 5 && a->rec.gaps == 0);       // sequence continued (snapshot re-bases the client)
  w.feed(MsgE(1, 1, 1)); w.pump();                              // a no longer wants trades
  CHECK(a->rec.trades.empty());
  CHECK(w.books.interest(1) == L2 && w.books.interest_depth(1) == 1);
  a->rec.clear(); b->rec.clear();

  // ── 6. unsubscribe ─────────────────────────────────────────────────────
  CHECK(b->c->unsubscribe({"AAPL", "MSFT"}, L2) != 0); w.pump();
  CHECK(b->rec.acks.size() == 1 && b->rec.acks[0].results[0].status == (uint8_t)Status::Ok);
  CHECK(b->rec.acks[0].results[1].status == (uint8_t)Status::NotSubscribed);   // MSFT was TNS only
  w.feed(MsgA(1, 10, 'B', 2, "AAPL", 1000700)); w.pump();
  CHECK(b->rec.l2.empty() && a->rec.l2.size() == 1);
  b->rec.clear();

  // ── 7. request validation: errors come back in the ack ──────────────────
  auto v = w.client(ipc_client());
  struct Case { uint8_t features, depth; Status want; };
  for (Case cs : {Case{L2, 11, Status::BadDepth}, Case{L2, 0, Status::BadDepth}, Case{0, 0, Status::BadFeatures}, Case{0x4, 0, Status::BadFeatures}, Case{TNS | 0x8, 0, Status::BadFeatures}}) {
    v->rec.clear(); CHECK(v->c->subscribe({"AAPL"}, cs.features, cs.depth) != 0); w.pump();
    CHECK(v->rec.acks.size() == 1 && v->rec.acks[0].status == cs.want && v->rec.l2.empty());
  }
  CHECK(w.books.interest(1) == L2);                                 // rejected requests change nothing

  // ── 8. multicast: one publication serves every client on the group ─────────
  const size_t sinks_before = w.server->sink_count();
  ClientConfig m1 = udp_client("10.0.0.3", 40456); m1.multicast_group = "239.1.1.1"; m1.reply_stream_id = 777;
  ClientConfig m2 = udp_client("10.0.0.4", 40456); m2.multicast_group = "239.1.1.1"; m2.reply_stream_id = 777;
  auto c1 = w.client(m1); auto c2 = w.client(m2);
  CHECK(c1->c->subscribe({"MSFT"}, TNS) != 0); CHECK(c2->c->subscribe({"MSFT"}, TNS) != 0); w.pump();

  CHECK(w.server->sink_count() == sinks_before + 1);
  const uint64_t sent0 = w.server->stats().trades_sent;
  w.feed(MsgP(2, 'B', 4, "MSFT", 3000200, 32)); w.pump();
  // b still follows MSFT time & sales on its own sink; the two group members share ONE publication.
  CHECK(w.server->stats().trades_sent == sent0 + 2);                // b + one multicast publish, not b + 2
  CHECK(!c1->rec.trades.empty() && !c2->rec.trades.empty());       // ...received by both group members
  c1->rec.clear(); c2->rec.clear();

  // A third member follows a different instrument on the same shared stream. The stream carries
  // the union, so each member must keep only what it subscribed to.
  ClientConfig m3 = udp_client("10.0.0.6", 40456); m3.multicast_group = "239.1.1.1"; m3.reply_stream_id = 777;
  auto c4 = w.client(m3);
  CHECK(c4->c->subscribe({"AAPL"}, TNS) != 0); w.pump();
  c1->rec.clear(); c2->rec.clear(); c4->rec.clear();
  w.feed(MsgP(1, 'S', 3, "AAPL", 1000900, 40)); w.feed(MsgP(2, 'B', 4, "MSFT", 3000400, 41)); w.pump();
  CHECK(c4->rec.trades.size() == 1 && c4->rec.trades[0].locate == 1);           // AAPL only
  CHECK(c1->rec.trades.size() == 1 && c1->rec.trades[0].locate == 2);           // MSFT only
  CHECK(c2->rec.trades.size() == 1 && c2->rec.trades[0].locate == 2);
  CHECK(c4->c->stats().filtered >= 1 && c1->c->stats().filtered >= 1);          // the other's data was seen and dropped
  CHECK(c1->rec.gaps == 0 && c2->rec.gaps == 0 && c4->rec.gaps == 0);
  c1->rec.clear(); c2->rec.clear(); c4->rec.clear();

  // ── 9. a unicast / IPC reply channel belongs to one client ───────────────
  { // a different client id asks for the reply stream client `a` already owns on aeron:ipc
    auto ctl = add_publication_blocking(w.tp, "aeron:ipc", 1001, std::chrono::milliseconds(100));
    SubscribeRequest r{}; r.client_id = 0xBAD; r.request_id = 1; r.features = TNS; r.symbol_count = 1;
    r.reply_stream_id = static_cast<uint32_t>(a->c->reply_stream_id());
    set_channel(r.reply_channel, "aeron:ipc"); set_symbol(r.symbols[0], "AAPL");
    uint8_t buf[kMaxMessageSize]; ctl->offer(buf, encode(r, buf));
    const uint64_t dropped0 = w.server->stats().dropped_unreplyable; const size_t clients0 = w.server->client_count();
    w.pump(1);
    CHECK(w.server->stats().dropped_unreplyable == dropped0 + 1 && w.server->client_count() == clients0);
    CHECK(a->rec.acks.empty()); }                                   // and nothing leaked to the owner

  // ── 10. malformed control traffic is counted and ignored ─────────────────
  { auto ctl = add_publication_blocking(w.tp, "aeron:ipc", 1001, std::chrono::milliseconds(100));
    const uint64_t m0 = w.server->stats().malformed;
    const uint8_t junk[] = {1, 2, 3}; ctl->offer(junk, sizeof junk);
    uint8_t bad[kMaxMessageSize] = {}; SubscribeRequest r{}; r.symbol_count = 0; encode(r, bad); ctl->offer(bad, sizeof r);
    w.pump(1);
    CHECK(w.server->stats().malformed == m0 + 2); }

  // ── 11. a reply channel nobody listens on is abandoned after the connect timeout ──
  { auto ctl = add_publication_blocking(w.tp, "aeron:ipc", 1001, std::chrono::milliseconds(100));
    SubscribeRequest r{}; r.client_id = 4242; r.request_id = 1; r.reply_stream_id = 31337; r.features = TNS; r.symbol_count = 1;
    set_channel(r.reply_channel, "aeron:udp?endpoint=10.9.9.9:40999"); set_symbol(r.symbols[0], "AAPL");
    uint8_t buf[kMaxMessageSize]; ctl->offer(buf, encode(r, buf));
    const size_t clients0 = w.server->client_count(), dropped0 = w.server->stats().clients_dropped;
    w.pump(1);
    CHECK(w.server->client_count() == clients0 + 1);
    for (int i = 0; i < 60; ++i) w.pump(1);                     // 6 s of fake time, more than connect_timeout
    CHECK(w.server->client_count() == clients0);                 // the unreachable client was removed again
    CHECK(w.server->stats().clients_dropped == dropped0 + 1); }

  // ── 12. clients that stop heartbeating are expired and their interest removed ──
  { w.rigs.erase(std::find(w.rigs.begin(), w.rigs.end(), b.get()));       // b stops polling -> no heartbeats
    CHECK(b->c->subscribe({"MSFT"}, TNS) != 0); w.pump();
    CHECK(w.books.interest(2) & TNS);
    const uint64_t exp0 = w.server->stats().clients_expired; const size_t clients0 = w.server->client_count(), sinks0 = w.server->sink_count();
    for (int i = 0; i < 110; ++i) w.pump(1);                                  // 11 s: everyone else keeps heartbeating
    CHECK(w.server->stats().clients_expired == exp0 + 1);                     // only the silent client
    CHECK(w.server->client_count() == clients0 - 1 && w.server->sink_count() == sinks0 - 1);   // its sink is released
    const size_t b_trades = b->rec.trades.size();
    w.feed(MsgP(2, 'B', 1, "MSFT", 3000300, 33)); w.pump(1);
    CHECK(b->rec.trades.size() == b_trades && !c1->rec.trades.empty());       // group still served, b is not
  }

  // ── 13. back-pressure drops (never blocks) and the client sees the gap ────
  { World w2;
    w2.feed(MsgR(1, "AAPL")); w2.feed(MsgA(1, 1, 'B', 10, "AAPL", 1000000));
    w2.tp.set_capacity(3);                                          // the next client's queue is tiny
    auto s = w2.client(ipc_client());
    CHECK(s->c->subscribe({"AAPL"}, L2, 1) != 0); w2.pump();
    for (uint64_t i = 0; i < 10; ++i) w2.feed(MsgA(1, 100 + i, 'B', 1, "AAPL", 1000100 + (uint32_t)i * 100));   // each is a new best bid
    CHECK(w2.server->stats().dropped_back_pressure > 0);
    w2.pump(1);                                                     // the client drains what fit
    w2.feed(MsgA(1, 200, 'B', 1, "AAPL", 1100000)); w2.pump(1);
    CHECK(s->rec.gaps > 0 && s->c->stats().gaps > 0); }

  // ── 14. client notices a silent server ─────────────────────────────────
  { World w3; w3.feed(MsgR(1, "AAPL"));
    auto s = w3.client(ipc_client()); CHECK(s->c->subscribe({"AAPL"}, TNS) != 0); w3.pump();
    for (int i = 0; i < 20; ++i) { g_now += 500'000'000ull; s->c->poll(s->rec); }     // server not polled for 10 s
    CHECK(s->rec.timeouts == 1); }

  // ── 15. destination allow-list ─────────────────────────────────────────
  { ServerConfig cfg; cfg.allowed_reply_cidrs = {*parse_cidr("10.0.0.0/8")};
    World w4(cfg); w4.feed(MsgR(1, "AAPL"));
    auto ok = w4.client(udp_client("10.1.2.3", 40300)); auto no = w4.client(udp_client("192.168.1.9", 40301));
    CHECK(ok->c->subscribe({"AAPL"}, TNS) != 0); CHECK(no->c->subscribe({"AAPL"}, TNS) != 0); w4.pump();
    CHECK(ok->rec.acks.size() == 1 && no->rec.acks.empty());
    CHECK(w4.server->stats().dropped_unreplyable == 1 && w4.server->client_count() == 1); }

  // ── 16. disconnect removes every subscription ───────────────────────────
  { const uint64_t id = v->c->client_id();
    CHECK(v->c->subscribe({"AAPL"}, TNS) != 0); w.pump();
    CHECK(w.books.interest(1) & TNS);
    CHECK(v->c->disconnect() != 0); w.pump();
    CHECK(!(w.books.interest(1) & TNS)); (void)id; }

  // ── 17. server shutdown clears all interest ─────────────────────────────
  w.server->stop();
  CHECK(w.books.interest(1) == 0 && w.books.interest(2) == 0);
  w.feed(MsgA(1, 99, 'B', 1, "AAPL", 1100000));                    // no listener: must not crash
  std::puts("ok");
  return 0;
}

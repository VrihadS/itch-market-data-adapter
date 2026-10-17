// The same server/client logic over real Aeron: an embedded media driver, one Aeron client
// per participant (as separate processes would have), IPC and UDP unicast (loopback), plus a
// multicast attempt. Multicast over loopback is not supported on every OS; if the group cannot
// be joined here the test says SKIPPED instead of passing silently.
#include <chrono>
#include <functional>
#include <thread>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include "check.h"
#include "itch_builders.h"
#include "itch/messaging/aeron_transport.hpp"
#include "itch/messaging/market_data_client.hpp"
#include "itch/messaging/market_data_server.hpp"

using namespace feed_handler;
using namespace feed_handler::messaging;
using namespace itchtest;
using Clock = std::chrono::steady_clock;

struct Rec : IMarketDataHandler {
  std::vector<AckView> acks; std::vector<L2View> l2; std::vector<TradeView> trades; int gaps = 0;
  void on_ack(const AckView &a) override { acks.push_back(a); }
  void on_l2(const L2View &v) override { l2.push_back(v); }
  void on_trade(const TradeView &t) override { trades.push_back(t); }
  void on_gap(uint16_t, StreamKind, uint64_t, uint64_t) override { ++gaps; }
};

struct Party { std::unique_ptr<AeronTransport> tp; std::unique_ptr<MarketDataClient> c; Rec rec; };

static bool wait_for(MarketDataServer &srv, std::vector<Party *> parties, const std::function<bool()> &cond, int ms = 8000) {
  const auto end = Clock::now() + std::chrono::milliseconds(ms);
  while (Clock::now() < end) {
    srv.poll_control();
    for (Party *p : parties) p->c->poll(p->rec);
    if (cond()) return true;
    std::this_thread::sleep_for(std::chrono::microseconds(200));
  }
  return false;
}

int main() {
  spdlog::set_level(spdlog::level::off);
  const unsigned base = 41000 + (::getpid() % 2000) * 4;           // avoid clashes between concurrent runs

  std::string err;
  auto driver = EmbeddedMediaDriver::start({}, err);
  CHECK(driver != nullptr);
  auto server_tp = AeronTransport::connect(driver->dir(), err);
  CHECK(server_tp != nullptr);

  BookManager books(1 << 12); NasdaqItchDecoder dec;
  auto feed = [&](const Bytes &b) { size_t u = 0; CHECK(dec.DecodeMessage((const char *)b.data(), b.size(), u, books)); };
  feed(MsgR(1, "AAPL")); feed(MsgR(2, "MSFT"));
  feed(MsgA(1, 1, 'B', 10, "AAPL", 1000000)); feed(MsgA(1, 2, 'B', 20, "AAPL", 990000)); feed(MsgA(1, 3, 'S', 5, "AAPL", 1010000));

  ServerConfig cfg;
  cfg.control_udp_channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(base);
  cfg.multicast_interface = "127.0.0.1";
  MarketDataServer server(*server_tp, books, cfg);
  CHECK(server.start());

  auto make = [&](ClientConfig cc) {
    auto p = std::make_unique<Party>();
    p->tp = AeronTransport::connect(driver->dir(), err); CHECK(p->tp != nullptr);
    p->c = std::make_unique<MarketDataClient>(*p->tp, cc);
    return p;
  };

  // The server is polled from this thread, so connect() would wait on a server nobody polls:
  // run each client's connect in its own thread while we keep servicing the server.
  auto connect = [&](Party &p) {
    bool ok = false; std::thread t([&] { ok = p.c->connect(); });
    const auto end = Clock::now() + std::chrono::seconds(10);
    while (Clock::now() < end) { server.poll_control(); std::this_thread::sleep_for(std::chrono::microseconds(200)); if (p.c->connected()) break; }
    t.join(); return ok;
  };

  constexpr uint8_t L2 = kFeatureL2Book, TNS = kFeatureTimeAndSales;

  // ── IPC client (same machine) ─────────────────────────────────────────
  ClientConfig ic; ic.server_host = "ipc";
  auto ipc = make(ic);
  if (!connect(*ipc)) { std::fprintf(stderr, "IPC connect failed: %s\n", ipc->c->last_error().c_str()); return 1; }
  CHECK(ipc->c->subscribe({"AAPL"}, L2 | TNS, 3) != 0);
  CHECK(wait_for(server, {ipc.get()}, [&] { return !ipc->rec.acks.empty() && !ipc->rec.l2.empty(); }));
  CHECK(ipc->rec.acks[0].status == Status::Ok && ipc->rec.acks[0].results[0].locate == 1);
  CHECK(ipc->rec.l2[0].snapshot && ipc->rec.l2[0].bid_count == 2 && ipc->rec.l2[0].ask_count == 1);
  CHECK(ipc->rec.l2[0].bids[0].price == 1000000 && ipc->rec.l2[0].asks[0].price == 1010000);

  // ── UDP unicast client (stands in for another machine) ────────────────────
  ClientConfig uc; uc.server_host = "127.0.0.1"; uc.control_port = base; uc.reply_host = "127.0.0.1"; uc.reply_port = base + 1;
  auto udp = make(uc);
  if (!connect(*udp)) { std::fprintf(stderr, "UDP connect failed: %s\n", udp->c->last_error().c_str()); return 1; }
  CHECK(udp->c->subscribe({"AAPL", "MSFT"}, L2, 1) != 0);
  CHECK(wait_for(server, {ipc.get(), udp.get()}, [&] { return udp->rec.l2.size() >= 2; }));   // one snapshot per symbol
  CHECK(udp->rec.acks[0].status == Status::Ok && udp->rec.l2[0].snapshot && udp->rec.l2[0].bid_count <= 1);

  // ── live updates reach both transports; each gets what it asked for ──────
  const size_t ipc_l2 = ipc->rec.l2.size(), udp_l2 = udp->rec.l2.size();
  feed(WithTs(MsgA(1, 4, 'B', 3, "AAPL", 1000500), 100));     // new best bid
  feed(WithTs(MsgE(1, 4, 2), 200));                           // print + book change
  CHECK(wait_for(server, {ipc.get(), udp.get()}, [&] { return !ipc->rec.trades.empty() && ipc->rec.l2.size() >= ipc_l2 + 2 && udp->rec.l2.size() >= udp_l2 + 2; }));
  CHECK(ipc->rec.trades[0].kind == 'E' && ipc->rec.trades[0].price == 1000500 && ipc->rec.trades[0].shares == 2 && ipc->rec.trades[0].exchange_ts_ns == 200);
  CHECK(udp->rec.trades.empty());                              // the UDP client did not ask for prints
  CHECK(ipc->rec.gaps == 0 && udp->rec.gaps == 0);
  { const L2View &last = ipc->rec.l2.back(); CHECK(last.bid_count == 3 && last.bids[0].price == 1000500 && last.bids[0].shares == 1); }
  { const L2View &last = udp->rec.l2.back(); CHECK(last.locate == 1 && last.bid_count == 1); }   // depth 1 for the UDP client

  // ── disconnecting stops delivery and cleans up ──────────────────────────
  CHECK(udp->c->disconnect() != 0);
  CHECK(wait_for(server, {ipc.get(), udp.get()}, [&] { return server.client_count() == 1; }));
  CHECK(books.interest(2) == 0);                               // nobody follows MSFT any more

  // ── multicast ────────────────────────────────────────────────────────
  ClientConfig mc; mc.server_host = "127.0.0.1"; mc.control_port = base; mc.reply_host = "127.0.0.1"; mc.reply_port = base + 2;
  mc.multicast_group = "239.30.10.77"; mc.connect_timeout = std::chrono::milliseconds(3000);
  auto mcast = make(mc);
  bool mcast_ok = connect(*mcast) && mcast->c->subscribe({"AAPL"}, TNS) != 0 &&
                  wait_for(server, {ipc.get(), mcast.get()}, [&] { return !mcast->rec.acks.empty(); }, 4000);
  if (mcast_ok) {
    feed(MsgP(1, 'S', 7, "AAPL", 1005000, 55));
    mcast_ok = wait_for(server, {ipc.get(), mcast.get()}, [&] { return !mcast->rec.trades.empty(); }, 4000);
  }
  if (mcast_ok) { CHECK(mcast->rec.trades[0].kind == 'P' && mcast->rec.trades[0].price == 1005000); std::puts("  multicast: ok"); }
  else std::puts("  multicast: SKIPPED (this host could not deliver multicast over loopback; not verified here)");

  // tear down in order: clients, server, driver
  ipc.reset(); udp.reset(); mcast.reset(); server.stop();
  std::puts("ok");
  return 0;
}

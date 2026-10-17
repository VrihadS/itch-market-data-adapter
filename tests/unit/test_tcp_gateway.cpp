// TcpGateway over real loopback TCP sockets, with the market data server and the gateway's upstream
// clients on the in-process transport and a fake clock. The "peer" below is a raw socket speaking the
// wire protocol, exactly what the Python client does.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>

#include "check.h"
#include "itch_builders.h"
#include "itch/messaging/inproc_transport.hpp"
#include "itch/messaging/market_data_server.hpp"
#include "itch/messaging/tcp_gateway.hpp"

using namespace feed_handler;
using namespace feed_handler::messaging;
using namespace itchtest;

static uint64_t g_now = 1'000'000'000ull;
static uint64_t fake_clock() { return g_now; }
constexpr uint8_t L2 = kFeatureL2Book, TNS = kFeatureTimeAndSales;

// A raw TCP peer.
struct Peer {
  int fd = -1;
  std::vector<uint8_t> buf;
  bool closed = false;
  struct Msg { MsgType type; std::vector<uint8_t> bytes; };
  std::vector<Msg> msgs;

  explicit Peer(uint16_t port, int rcvbuf = 0) {
    fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (rcvbuf) setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof rcvbuf);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(port); a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(::connect(fd, (sockaddr *)&a, sizeof a) == 0);
  }
  ~Peer() { if (fd >= 0) ::close(fd); }
  void send(const void *d, size_t n) { CHECK(::send(fd, d, n, 0) == (ssize_t)n); }
  void send(const std::vector<uint8_t> &v) { send(v.data(), v.size()); }
  void drain() {   // read whatever has arrived, split into protocol messages
    uint8_t tmp[65536];
    for (;;) {
      const ssize_t n = ::recv(fd, tmp, sizeof tmp, MSG_DONTWAIT);
      if (n > 0) buf.insert(buf.end(), tmp, tmp + n);
      else { if (n == 0) closed = true; break; }
    }
    size_t off = 0;
    while (buf.size() - off >= 4) {
      Header h; std::memcpy(&h, buf.data() + off, 4);
      CHECK(h.length >= 4 && h.length <= kMaxMessageSize);
      if (buf.size() - off < h.length) break;
      msgs.push_back({(MsgType)h.type, std::vector<uint8_t>(buf.begin() + off, buf.begin() + off + h.length)});
      off += h.length;
    }
    buf.erase(buf.begin(), buf.begin() + off);
  }
  size_t count(MsgType t) const { size_t n = 0; for (auto &m : msgs) n += m.type == t; return n; }
  std::vector<Msg> of(MsgType t) const { std::vector<Msg> v; for (auto &m : msgs) if (m.type == t) v.push_back(m); return v; }
};

static std::vector<uint8_t> subscribe_msg(uint64_t cid, uint32_t rid, std::vector<std::string> syms, uint8_t features, uint8_t depth,
                                          const char *channel = "tcp") {
  SubscribeRequest r{}; r.client_id = cid; r.request_id = rid; r.features = features; r.depth = depth; r.symbol_count = (uint8_t)syms.size();
  CHECK(set_channel(r.reply_channel, channel));
  for (size_t i = 0; i < syms.size(); ++i) CHECK(set_symbol(r.symbols[i], syms[i]));
  uint8_t b[kMaxMessageSize]; const size_t n = encode(r, b); return {b, b + n};
}
static std::vector<uint8_t> unsubscribe_msg(uint64_t cid, uint32_t rid, std::vector<std::string> syms, uint8_t features) {
  UnsubscribeRequest r{}; r.client_id = cid; r.request_id = rid; r.features = features; r.symbol_count = (uint8_t)syms.size();
  for (size_t i = 0; i < syms.size(); ++i) CHECK(set_symbol(r.symbols[i], syms[i]));
  uint8_t b[kMaxMessageSize]; const size_t n = encode(r, b); return {b, b + n};
}

struct World {
  InprocTransport tp; BookManager books{1 << 12}; NasdaqItchDecoder dec;
  std::unique_ptr<MarketDataServer> server; std::unique_ptr<TcpGateway> gw;

  explicit World(std::function<void(GatewayConfig &)> tweak = nullptr, ServerConfig sc = {}) {
    sc.control_udp_channel = "aeron:udp?endpoint=10.0.0.1:40100"; sc.now_ns = fake_clock;
    server = std::make_unique<MarketDataServer>(tp, books, sc);
    CHECK(server->start());
    GatewayConfig gc; gc.listen_port = 0; gc.now_ns = fake_clock;
    gc.upstream.server_host = "ipc"; gc.upstream.now_ns = fake_clock;
    gc.upstream.connect_timeout = std::chrono::milliseconds(200);
    if (tweak) tweak(gc);
    gw = std::make_unique<TcpGateway>(tp, gc);
    std::string err; CHECK(gw->start(err));
  }
  void feed(const Bytes &b) { size_t u = 0; CHECK(dec.DecodeMessage((const char *)b.data(), b.size(), u, books)); }
  void pump(std::vector<Peer *> peers = {}, int rounds = 4, uint64_t step_ns = 100'000'000ull) {
    for (int i = 0; i < rounds; ++i) {
      g_now += step_ns;
      server->poll_control();
      gw->poll();
      std::this_thread::sleep_for(std::chrono::milliseconds(2));       // let loopback TCP deliver
      for (Peer *p : peers) p->drain();
    }
  }
};

static SubscribeAck ack_of(const Peer::Msg &m) { SubscribeAck a; CHECK(decode(m.bytes.data(), m.bytes.size(), a)); return a; }

int main() {
  spdlog::set_level(spdlog::level::off);
  {
    // ── 1. subscribe over TCP: ack carries the peer's own ids, then a snapshot, then live data ──
    World w;
    w.feed(MsgR(1, "AAPL")); w.feed(MsgR(2, "MSFT"));
    w.feed(MsgA(1, 1, 'B', 10, "AAPL", 1000000)); w.feed(MsgA(1, 2, 'B', 20, "AAPL", 990000)); w.feed(MsgA(1, 3, 'S', 5, "AAPL", 1010000));
    Peer p(w.gw->port());
    // the reply channel names an address the server is not allowed to send to: it must be ignored entirely
    ServerConfig dummy;
    p.send(subscribe_msg(0xABCDEF, 77, {"AAPL", "NOPE"}, L2 | TNS, 2, "aeron:udp?endpoint=8.8.8.8:1"));
    w.pump({&p});
    CHECK(p.count(MsgType::SubscribeAck) == 1 && p.count(MsgType::L2Book) == 1);
    { const SubscribeAck a = ack_of(p.of(MsgType::SubscribeAck)[0]);
      CHECK(a.request_id == 77 && a.client_id == 0xABCDEF && a.status == 0 && a.result_count == 2);
      CHECK(a.results[0].locate == 1 && a.results[0].status == 0 && a.results[1].status == (uint8_t)Status::UnknownSymbol); }
    CHECK(p.msgs[0].type == MsgType::SubscribeAck && p.msgs[1].type == MsgType::L2Book);       // ack before snapshot
    { L2Book h; BookLevel b[kMaxL2Depth], a[kMaxL2Depth];
      CHECK(decode_l2(p.of(MsgType::L2Book)[0].bytes.data(), p.of(MsgType::L2Book)[0].bytes.size(), h, b, a));
      CHECK(h.snapshot == 1 && h.locate == 1 && h.sequence == 1 && h.bid_count == 2 && h.ask_count == 1);
      CHECK(b[0].price == 1000000 && b[0].shares == 10 && b[1].price == 990000 && a[0].price == 1010000); }
    CHECK(w.server->client_count() == 1);
    p.msgs.clear();

    w.feed(WithTs(MsgA(1, 4, 'B', 3, "AAPL", 1000500), 500));        // new best bid
    w.feed(WithTs(MsgE(1, 4, 2), 600));                             // print, then book change
    w.pump({&p});
    CHECK(p.count(MsgType::L2Book) == 2 && p.count(MsgType::Trade) == 1);
    { Trade t; CHECK(decode(p.of(MsgType::Trade)[0].bytes.data(), p.of(MsgType::Trade)[0].bytes.size(), t));
      CHECK(t.kind == 'E' && t.side == 'B' && t.price == 1000500 && t.shares == 2 && t.sequence == 1 && t.exchange_ts_ns == 600); }
    p.msgs.clear();

    // ── 2. several requests: acks keep each request's own id; unsubscribe drops only what was asked ──
    p.send(subscribe_msg(0xABCDEF, 78, {"MSFT"}, TNS, 0));
    p.send(unsubscribe_msg(0xABCDEF, 79, {"AAPL"}, L2));
    w.pump({&p});
    { auto acks = p.of(MsgType::SubscribeAck); CHECK(acks.size() == 2);
      CHECK(ack_of(acks[0]).request_id == 78 && ack_of(acks[1]).request_id == 79); }
    CHECK(w.books.interest(1) == TNS && (w.books.interest(2) & TNS));
    p.msgs.clear();
    w.feed(MsgA(1, 5, 'B', 1, "AAPL", 1001000));                     // L2 no longer wanted
    w.feed(MsgP(2, 'B', 4, "MSFT", 3000200, 9)); w.pump({&p});
    CHECK(p.count(MsgType::L2Book) == 0 && p.count(MsgType::Trade) == 1);
    p.msgs.clear();

    // ── 3. heartbeats keep flowing ─────────────────────────────────────
    w.pump({&p}, 12);
    CHECK(p.count(MsgType::Heartbeat) >= 1);
    { Heartbeat hb; auto h = p.of(MsgType::Heartbeat)[0]; CHECK(decode(h.bytes.data(), h.bytes.size(), hb) && hb.from_server == 1 && hb.client_id == 0xABCDEF); }

    // ── 4. a clean disconnect: acked, then the gateway closes the socket and the server forgets us ──
    p.msgs.clear();
    p.send(unsubscribe_msg(0xABCDEF, 80, {}, 0));
    w.pump({&p}, 6);
    CHECK(p.count(MsgType::SubscribeAck) == 1 && ack_of(p.of(MsgType::SubscribeAck)[0]).request_id == 80);
    CHECK(p.closed);
    CHECK(w.server->client_count() == 0 && w.books.interest(1) == 0 && w.books.interest(2) == 0);
    CHECK(w.gw->connection_count() == 0);
  }
  {
    // ── 5. an abrupt TCP close releases the subscriptions right away (no 10 s wait) ──
    World w; w.feed(MsgR(1, "AAPL"));
    { Peer p(w.gw->port()); p.send(subscribe_msg(1, 1, {"AAPL"}, TNS, 0)); w.pump({&p}); CHECK(w.server->client_count() == 1); }
    w.pump({}, 3);
    CHECK(w.gw->connection_count() == 0 && w.server->client_count() == 0 && w.books.interest(1) == 0);
  }
  {
    // ── 6. malformed or hostile input closes the connection, nothing else is affected ──
    World w; w.feed(MsgR(1, "AAPL"));
    Peer good(w.gw->port()); good.send(subscribe_msg(1, 1, {"AAPL"}, TNS, 0)); w.pump({&good});
    { Peer bad(w.gw->port()); uint8_t junk[] = {0x88, 0x13, 1, 1, 0, 0, 0, 0}; bad.send(junk, sizeof junk);          // length 5000
      w.pump({&bad}, 3); CHECK(bad.closed); }
    { Peer bad(w.gw->port()); L2Book h{}; BookLevel lv[kMaxL2Depth] = {}; uint8_t b[kMaxL2MessageSize]; bad.send(b, encode_l2(h, lv, lv, b));   // server-to-client message
      w.pump({&bad}, 3); CHECK(bad.closed); }
    { Peer bad(w.gw->port()); auto m = subscribe_msg(2, 1, {"AAPL"}, TNS, 0); m[2] = 99; bad.send(m);               // wrong version
      w.pump({&bad}, 3); CHECK(bad.closed); }
    { Peer bad(w.gw->port()); auto m = subscribe_msg(2, 1, {"AAPL"}, TNS, 0); m[4 + 8 + 4 + 4 + 3] = 0; m[24] = 0; m[25] = 0; // symbol_count/channel corrupted
      std::memset(&m[24], 0xFF, 96); bad.send(m); w.pump({&bad}, 3); CHECK(bad.closed); }                           // channel without NUL
    CHECK(w.gw->stats().closed_malformed >= 4);
    CHECK(!good.closed && w.gw->connection_count() == 1);
    w.feed(MsgP(1, 'B', 1, "AAPL", 1000000, 5)); w.pump({&good});
    CHECK(good.count(MsgType::Trade) == 1);                                                                          // the good peer was unaffected
  }
  {
    // ── 7. peer allow-list and connection limit ─────────────────────────
    World w([](GatewayConfig &c) { c.allowed_peers = {*parse_cidr("10.0.0.0/8")}; });
    Peer p(w.gw->port()); w.pump({&p}, 3);
    CHECK(p.closed && w.gw->stats().rejected_peer == 1 && w.gw->connection_count() == 0);
    World w2([](GatewayConfig &c) { c.max_connections = 2; });
    Peer a(w2.gw->port()), b(w2.gw->port()), c(w2.gw->port()); w2.pump({&a, &b, &c}, 3);
    CHECK(c.closed && !a.closed && !b.closed && w2.gw->stats().rejected_full == 1 && w2.gw->connection_count() == 2);
  }
  {
    // ── 8. a peer that cannot keep up is cut off without hurting the others ──
    World w([](GatewayConfig &c) { c.max_pending_output_bytes = 64 * 1024; });
    w.feed(MsgR(1, "AAPL")); w.feed(MsgA(1, 1, 'B', 10, "AAPL", 1000000));
    Peer slow(w.gw->port(), 4096), fast(w.gw->port());
    slow.send(subscribe_msg(1, 1, {"AAPL"}, L2, 10)); fast.send(subscribe_msg(2, 1, {"AAPL"}, L2, 10)); w.pump({&slow, &fast});
    CHECK(w.gw->connection_count() == 2);
    for (uint32_t i = 0; i < 6000 && w.gw->stats().closed_slow == 0; ++i) {       // `slow` never reads
      w.feed(MsgA(1, 100 + i, 'B', 1, "AAPL", 1000100 + i)); w.gw->poll();
      if (i % 50 == 0) fast.drain();
    }
    w.pump({&fast}, 3);
    CHECK(w.gw->stats().closed_slow == 1 && w.gw->connection_count() == 1);
    CHECK(!fast.closed && fast.count(MsgType::L2Book) > 100);
  }
  {
    // ── 9. no market data server: the peer is told, then closed ──────────
    World w([](GatewayConfig &c) { c.upstream.server_host = "10.9.9.9"; c.upstream.reply_host = "10.9.9.8"; c.upstream.connect_timeout = std::chrono::milliseconds(80); });
    Peer p(w.gw->port()); p.send(subscribe_msg(5, 9, {"AAPL"}, TNS, 0)); w.pump({&p}, 5);
    CHECK(p.count(MsgType::SubscribeAck) == 1);
    { const SubscribeAck a = ack_of(p.of(MsgType::SubscribeAck)[0]); CHECK(a.request_id == 9 && a.status == (uint8_t)Status::Unavailable); }
    CHECK(p.closed && w.gw->stats().upstream_failed == 1);
  }
  {
    // ── 10. idle peers are dropped, peers that heartbeat are not ─────────
    World w; w.feed(MsgR(1, "AAPL"));
    Peer idle(w.gw->port()), alive(w.gw->port());
    idle.send(subscribe_msg(1, 1, {"AAPL"}, TNS, 0)); alive.send(subscribe_msg(2, 1, {"AAPL"}, TNS, 0)); w.pump({&idle, &alive});
    for (int i = 0; i < 40; ++i) {                                                 // 40 s of fake time
      Heartbeat hb{}; hb.client_id = 2; uint8_t b[kMaxMessageSize]; alive.send(b, encode(hb, b));
      w.pump({&idle, &alive}, 1, 1'000'000'000ull);
    }
    CHECK(idle.closed && !alive.closed && w.gw->stats().closed_idle == 1);
  }
  {
    // ── 11. when the server goes quiet the gateway drops its peers so they can reconnect ──
    World w([](GatewayConfig &c) { c.upstream.server_timeout_ns = 2'000'000'000ull; });
    w.feed(MsgR(1, "AAPL"));
    Peer p(w.gw->port()); p.send(subscribe_msg(1, 1, {"AAPL"}, TNS, 0)); w.pump({&p});
    CHECK(p.count(MsgType::SubscribeAck) == 1);
    for (int i = 0; i < 30; ++i) { g_now += 500'000'000ull; w.gw->poll(); std::this_thread::sleep_for(std::chrono::milliseconds(1)); p.drain(); }   // server never polled
    CHECK(p.closed && w.gw->stats().closed_upstream == 1);
  }
  std::puts("ok");
  return 0;
}

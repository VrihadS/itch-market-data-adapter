// Wire protocol: round trips, strict rejection of malformed input, and a fuzz pass.
#include <cstddef>
#include <cstring>
#include <fstream>
#include <map>
#include <random>
#include <sstream>
#include <vector>

#include "check.h"
#include "itch/messaging/channel.hpp"
#include "itch/messaging/protocol.hpp"

using namespace feed_handler::messaging;

static SubscribeRequest valid_request() {
  SubscribeRequest r{};
  r.client_id = 0x1122334455667788ull; r.request_id = 7; r.reply_stream_id = 4242;
  r.features = kFeatureL2Book | kFeatureTimeAndSales; r.depth = 10; r.symbol_count = 2;
  CHECK(set_channel(r.reply_channel, "aeron:udp?endpoint=10.0.0.5:40200"));
  CHECK(set_symbol(r.symbols[0], "AAPL")); CHECK(set_symbol(r.symbols[1], "BRK.B"));
  return r;
}

static void round_trips() {
  uint8_t buf[kMaxMessageSize];
  { SubscribeRequest in = valid_request(), out{}; const size_t n = encode(in, buf);
    CHECK(n == sizeof(SubscribeRequest) && decode(buf, n, out));
    CHECK(out.client_id == in.client_id && out.request_id == 7 && out.reply_stream_id == 4242 && out.depth == 10);
    CHECK(*get_symbol(out.symbols[0]) == "AAPL" && *get_symbol(out.symbols[1]) == "BRK.B");
    CHECK(*get_channel(out.reply_channel) == "aeron:udp?endpoint=10.0.0.5:40200"); }
  { UnsubscribeRequest in{}, out{}; in.client_id = 9; in.request_id = 3; in.features = kFeatureL2Book; in.symbol_count = 1;
    CHECK(set_symbol(in.symbols[0], "MSFT")); const size_t n = encode(in, buf);
    CHECK(decode(buf, n, out) && out.client_id == 9 && out.symbol_count == 1); }
  { SubscribeAck in{}, out{}; in.client_id = 5; in.request_id = 6; in.status = 0; in.result_count = 2;
    in.results[0] = {13, (uint8_t)Status::Ok, 0}; in.results[1] = {0, (uint8_t)Status::UnknownSymbol, 0};
    const size_t n = encode(in, buf); CHECK(decode(buf, n, out) && out.results[0].locate == 13 && out.results[1].status == 1); }
  { Heartbeat in{}, out{}; in.client_id = 4; in.send_ts_ns = 99; in.from_server = 1; const size_t n = encode(in, buf);
    CHECK(n == sizeof(Heartbeat) && decode(buf, n, out) && out.send_ts_ns == 99 && out.from_server == 1); }
  { Trade in{}, out{}; in.locate = 13; in.kind = 'Q'; in.side = 0; in.cross_type = 'C'; in.sequence = 1ull << 40;
    in.exchange_ts_ns = 34200000000000ull; in.price = -1; in.shares = 5000000000ull; in.match_number = 77;
    const size_t n = encode(in, buf); CHECK(decode(buf, n, out));
    CHECK(out.kind == 'Q' && out.cross_type == 'C' && out.sequence == (1ull << 40) && out.shares == 5000000000ull && out.price == -1); }
  { L2Book in{}, out{}; BookLevel b[kMaxL2Depth], a[kMaxL2Depth], ob[kMaxL2Depth], oa[kMaxL2Depth];
    in.locate = 13; in.bid_count = 3; in.ask_count = 2; in.snapshot = 1; in.sequence = 5; in.exchange_ts_ns = 1; in.send_ts_ns = 2;
    for (int i = 0; i < 3; ++i) b[i] = {1000000 - i * 100, (uint32_t)i + 1, (uint64_t)(i + 1) * 100};
    for (int i = 0; i < 2; ++i) a[i] = {1000100 + i * 100, (uint32_t)i + 1, (uint64_t)(i + 1) * 10};
    const size_t n = encode_l2(in, b, a, buf); CHECK(n == sizeof(L2Book) + 5 * sizeof(BookLevel));
    CHECK(decode_l2(buf, n, out, ob, oa) && out.bid_count == 3 && out.ask_count == 2 && out.snapshot == 1);
    CHECK(ob[2].price == 999800 && ob[2].shares == 300 && oa[1].price == 1000200 && oa[1].orders == 2); }
}

static void rejects_malformed() {
  uint8_t buf[kMaxMessageSize]; MsgType t; SubscribeRequest out{};
  const SubscribeRequest good = valid_request(); const size_t n = encode(good, buf);
  CHECK(decode(buf, n, out));
  CHECK(!decode(buf, n - 1, out) && !decode(buf, 0, out) && !decode(nullptr, 0, out));    // short / empty
  { std::vector<uint8_t> big(buf, buf + n); big.push_back(0); CHECK(!decode(big.data(), big.size(), out)); }   // length mismatch
  { uint8_t c[kMaxMessageSize]; std::memcpy(c, buf, n); c[2] = 99; CHECK(!peek_header(c, n, t)); }            // version
  { uint8_t c[kMaxMessageSize]; std::memcpy(c, buf, n); c[3] = 0; CHECK(!peek_header(c, n, t)); }             // type 0
  { uint8_t c[kMaxMessageSize]; std::memcpy(c, buf, n); c[3] = 250; CHECK(!peek_header(c, n, t)); }           // type unknown
  { SubscribeRequest r = good; r.symbol_count = 0; encode(r, buf); CHECK(!decode(buf, sizeof r, out)); }
  { SubscribeRequest r = good; r.symbol_count = 33; encode(r, buf); CHECK(!decode(buf, sizeof r, out)); }
  { SubscribeRequest r = good; std::memset(r.reply_channel, 'x', sizeof r.reply_channel); encode(r, buf); CHECK(!decode(buf, sizeof r, out)); }  // no NUL
  { SubscribeRequest r = good; r.symbols[0][0] = '\n'; encode(r, buf); CHECK(!decode(buf, sizeof r, out)); }   // control char in symbol
  { SubscribeRequest r = good; std::memcpy(r.symbols[0], "AB\0CDEFG", 8); encode(r, buf); CHECK(!decode(buf, sizeof r, out)); }   // data after padding
  { SubscribeAck a{}; a.result_count = 33; uint8_t b2[kMaxMessageSize]; encode(a, b2); SubscribeAck o; CHECK(!decode(b2, sizeof a, o)); }
  { // a hostile L2 message claiming 11 levels (patched in at the byte level; encode_l2 itself clamps)
    L2Book h{}; BookLevel lv[kMaxL2Depth] = {}; uint8_t b2[512] = {};
    h.bid_count = 10; size_t m = encode_l2(h, lv, lv, b2);
    m += sizeof(BookLevel); b2[offsetof(L2Book, bid_count)] = 11;                       // one more level than allowed...
    const uint16_t newlen = static_cast<uint16_t>(m); std::memcpy(b2, &newlen, 2);       // ...with a consistent length
    L2Book o; BookLevel x[kMaxL2Depth], y[kMaxL2Depth]; CHECK(!decode_l2(b2, m, o, x, y));
    L2Book big{}; big.bid_count = 200; big.ask_count = 200; uint8_t b3[1024] = {}; BookLevel many[kMaxL2Depth] = {};
    const size_t k = encode_l2(big, many, many, b3); CHECK(k == sizeof(L2Book) + 2 * kMaxL2Depth * sizeof(BookLevel)); }   // clamped
  { L2Book h{}; h.bid_count = 2; BookLevel lv[kMaxL2Depth] = {}; uint8_t b2[512];
    const size_t m = encode_l2(h, lv, lv, b2); L2Book o; BookLevel x[kMaxL2Depth], y[kMaxL2Depth];
    CHECK(decode_l2(b2, m, o, x, y)); CHECK(!decode_l2(b2, m - 1, o, x, y)); }                            // truncated levels
  // symbol helpers
  char s[kSymbolLen];
  CHECK(!set_symbol(s, "") && !set_symbol(s, "TOOLONGSYM") && !set_symbol(s, "A B") && !set_symbol(s, "A\tB"));
  CHECK(set_symbol(s, "A") && *get_symbol(s) == "A");
}

static void fuzz() {   // random bytes and mutated valid messages must never crash or over-read (run under ASan)
  std::mt19937_64 g(1);
  uint8_t src[kMaxMessageSize]; encode(valid_request(), src);
  size_t accepted = 0;
  for (int i = 0; i < 300000; ++i) {
    std::vector<uint8_t> m(g() % (sizeof(SubscribeRequest) + 40));
    if (i & 1) { m.assign(src, src + sizeof(SubscribeRequest)); for (int k = 0, nk = 1 + g() % 6; k < nk; ++k) m[g() % m.size()] = (uint8_t)g(); if (g() % 4 == 0) m.resize(g() % m.size()); }
    else for (auto &b : m) b = (uint8_t)g();
    MsgType t; SubscribeRequest r; UnsubscribeRequest u; SubscribeAck a; Heartbeat h; Trade tr; L2Book l; BookLevel x[kMaxL2Depth], y[kMaxL2Depth];
    peek_header(m.data(), m.size(), t);
    accepted += decode(m.data(), m.size(), r) + decode(m.data(), m.size(), u) + decode(m.data(), m.size(), a) +
                decode(m.data(), m.size(), h) + decode(m.data(), m.size(), tr) + decode_l2(m.data(), m.size(), l, x, y);
    if (decode(m.data(), m.size(), r)) { CHECK(r.symbol_count >= 1 && r.symbol_count <= kMaxSymbolsPerRequest); CHECK(get_channel(r.reply_channel)); }
  }
  std::printf("  fuzz: 300000 inputs handled, %zu accepted by some decoder\n", accepted);
}

static void channels() {
  using K = ChannelKind;
  auto kind = [](const char *s) { return parse_reply_channel(s).kind; };
  CHECK(kind("aeron:ipc") == K::Ipc);
  CHECK(kind("aeron:udp?endpoint=10.0.0.5:40200") == K::UdpUnicast);
  CHECK(kind("aeron:udp?endpoint=127.0.0.1:1") == K::UdpUnicast);
  CHECK(kind("aeron:udp?endpoint=239.1.1.1:40456") == K::UdpMulticast);
  CHECK(kind("aeron:udp?endpoint=224.0.0.1:9") == K::UdpMulticast);
  const auto pc = parse_reply_channel("aeron:udp?endpoint=192.168.1.20:65535");
  CHECK(pc.port == 65535 && pc.ip == (192u << 24 | 168u << 16 | 1u << 8 | 20u));
  for (const char *bad : {
         "", "aeron:", "aeron:ipc ", " aeron:ipc", "aeron:ipc|term-length=64k", "aeron:ipc?alias=x",
         "aeron:udp", "aeron:udp?endpoint=", "aeron:udp?endpoint=10.0.0.5", "aeron:udp?endpoint=10.0.0.5:",
         "aeron:udp?endpoint=10.0.0.5:0", "aeron:udp?endpoint=10.0.0.5:65536", "aeron:udp?endpoint=10.0.0.5:99999999",
         "aeron:udp?endpoint=10.0.0.5:40200|term-length=64k", "aeron:udp?endpoint=10.0.0.5:40200|control=10.0.0.9:1",
         "aeron:udp?endpoint=10.0.0.5:40200&interface=eth0", "aeron:udp?endpoint=10.0.0.5:40200 ",
         "aeron:udp?endpoint=localhost:40200", "aeron:udp?endpoint=example.com:40200", "aeron:udp?endpoint=[::1]:40200",
         "aeron:udp?endpoint=10.0.0.256:40200", "aeron:udp?endpoint=10.0.0:40200", "aeron:udp?endpoint=10.0.0.5.6:40200",
         "aeron:udp?endpoint=010.0.0.5:40200", "aeron:udp?endpoint=0.0.0.0:40200", "aeron:udp?endpoint=255.255.255.255:40200",
         "aeron:udp?endpoint=240.0.0.1:40200", "aeron-spy:aeron:ipc", "AERON:IPC", "aeron:udp?endpoint=10.0.0.5:4\n0200"})
    CHECK(kind(bad) == K::Invalid);

  CHECK(*parse_ipv4("1.2.3.4") == 0x01020304u && !parse_ipv4("1.2.3") && !parse_ipv4("1.2.3.4.5") && !parse_ipv4("a.b.c.d"));
  CHECK(format_ipv4(0x0A000005u) == "10.0.0.5");
  const auto c = parse_cidr("10.1.2.0/24"); CHECK(c && c->contains(*parse_ipv4("10.1.2.77")) && !c->contains(*parse_ipv4("10.1.3.1")));
  const auto h = parse_cidr("10.0.0.5");    CHECK(h && h->contains(*parse_ipv4("10.0.0.5")) && !h->contains(*parse_ipv4("10.0.0.6")));
  const auto all = parse_cidr("0.0.0.0/0"); CHECK(all && all->contains(*parse_ipv4("8.8.8.8")));
  CHECK(!parse_cidr("10.0.0.0/33") && !parse_cidr("10.0.0.0/") && !parse_cidr("x/8"));
}

// The byte layout is a contract with non-C++ clients: the encoder must reproduce the shared vectors.
static std::vector<uint8_t> unhex(const std::string &h) {
  std::vector<uint8_t> v; for (size_t i = 0; i + 1 < h.size(); i += 2) v.push_back((uint8_t)std::stoi(h.substr(i, 2), nullptr, 16)); return v; }

static void golden_vectors() {
  std::ifstream f(ITCH_WIRE_VECTORS); CHECK(f.good());
  std::map<std::string, std::vector<uint8_t>> vec; std::string line;
  while (std::getline(f, line)) {
    if (line.empty() || line[0] == '#') continue;
    const auto eq = line.find(" = \""); CHECK(eq != std::string::npos);
    vec[line.substr(0, eq)] = unhex(line.substr(eq + 4, line.size() - eq - 5));
  }
  CHECK(vec.size() == 6);
  uint8_t buf[kMaxMessageSize];
  auto same = [&](const char *name, size_t n) { return vec.at(name).size() == n && std::memcmp(vec.at(name).data(), buf, n) == 0; };
  { SubscribeRequest r{}; r.client_id = 0x1122334455667788ull; r.request_id = 7; r.features = 3; r.depth = 5; r.symbol_count = 2;
    set_channel(r.reply_channel, "tcp"); set_symbol(r.symbols[0], "AAPL"); set_symbol(r.symbols[1], "BRK.B");
    CHECK(same("SUBSCRIBE", encode(r, buf))); }
  { UnsubscribeRequest r{}; r.client_id = 0x1122334455667788ull; r.request_id = 8; r.features = 2; r.symbol_count = 1; set_symbol(r.symbols[0], "MSFT");
    CHECK(same("UNSUBSCRIBE", encode(r, buf))); }
  { Heartbeat h{}; h.client_id = 0x1122334455667788ull; h.send_ts_ns = 1700000000123456789ull; h.from_server = 1; CHECK(same("HEARTBEAT", encode(h, buf))); }
  { SubscribeAck a{}; a.client_id = 0x1122334455667788ull; a.request_id = 7; a.result_count = 2; a.results[0] = {13, 0, 0}; a.results[1] = {0, 1, 0};
    CHECK(same("ACK", encode(a, buf))); }
  { L2Book h{}; BookLevel b[3] = {{2869300, 2, 201}, {2869200, 3, 300}, {2869100, 1, 5}}, a[2] = {{2869500, 1, 100}, {2869600, 4, 400}};
    h.locate = 13; h.bid_count = 3; h.ask_count = 2; h.snapshot = 1; h.sequence = 42; h.exchange_ts_ns = 36000123456789ull; h.send_ts_ns = 1700000000999999999ull;
    CHECK(same("L2", encode_l2(h, b, a, buf))); }
  { Trade t{}; t.locate = 13; t.kind = 'Q'; t.cross_type = 'C'; t.sequence = 9; t.exchange_ts_ns = 57600000000000ull; t.send_ts_ns = 1700000001000000000ull;
    t.price = 2870000; t.shares = 5000000000ull; t.match_number = 123456; CHECK(same("TRADE", encode(t, buf))); }
  std::puts("  golden wire vectors: C++ encoder reproduces all 6");
}

int main() {
  golden_vectors();
  round_trips();
  rejects_malformed();
  fuzz();
  channels();
  std::puts("ok");
  return 0;
}

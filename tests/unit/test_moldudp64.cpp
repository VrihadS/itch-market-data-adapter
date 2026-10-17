// MoldUDP64 packet layer: spec layout, strict whole-packet validation, builder, fuzz.
#include <cstring>
#include <random>
#include <vector>

#include "check.h"
#include "itch/net/moldudp64.hpp"

using namespace feed_handler::mold;

static std::vector<uint8_t> packet(const Session &s, uint64_t seq, const std::vector<std::vector<uint8_t>> &msgs) {
  std::vector<uint8_t> b(kHeaderSize, 0);
  write_header(b.data(), s, seq, (uint16_t)msgs.size());
  for (auto &m : msgs) { uint8_t l[2]; itch::write_be16(l, (uint16_t)m.size()); b.insert(b.end(), l, l + 2); b.insert(b.end(), m.begin(), m.end()); }
  return b;
}

int main() {
  const Session s("SESSION001");
  CHECK(s.str() == "SESSION001" && Session("AB").str() == "AB" && Session("AB") != Session("AC"));

  { // layout straight from the spec: session@0(10) sequence@10(8) count@18(2), then [len(2) data]*
    auto p = packet(s, 0x0102030405060708ull, {{'a', 'b', 'c'}, {}, {'x'}});
    CHECK(std::memcmp(p.data(), "SESSION001", 10) == 0);
    const uint8_t seq[8] = {1, 2, 3, 4, 5, 6, 7, 8}; CHECK(std::memcmp(p.data() + 10, seq, 8) == 0);
    CHECK(p[18] == 0 && p[19] == 3 && p[20] == 0 && p[21] == 3 && p[22] == 'a');
    Packet pk; CHECK(parse_packet(p.data(), p.size(), pk) == ParseError::None);
    CHECK(pk.kind == PacketKind::Data && pk.first_sequence() == 0x0102030405060708ull && pk.message_count() == 3);
    CHECK(pk.end_sequence() == 0x0102030405060708ull + 3 && pk.header.session == s);
    BlockIterator it(pk); const uint8_t *d; uint16_t n;
    CHECK(it.next(d, n) && n == 3 && std::memcmp(d, "abc", 3) == 0);
    CHECK(it.next(d, n) && n == 0 && d == nullptr);                 // zero-length message is legal and keeps its sequence number
    CHECK(it.next(d, n) && n == 1 && *d == 'x');
    CHECK(!it.next(d, n)); }

  { // heartbeat and end of session: next expected sequence, no payload
    uint8_t b[64]; Packet pk;
    CHECK(build_heartbeat(b, s, 4242) == kHeaderSize && parse_packet(b, kHeaderSize, pk) == ParseError::None);
    CHECK(pk.kind == PacketKind::Heartbeat && pk.end_sequence() == 4242 && pk.message_count() == 0);
    CHECK(build_end_of_session(b, s, 99) == kHeaderSize && parse_packet(b, kHeaderSize, pk) == ParseError::None);
    CHECK(pk.kind == PacketKind::EndOfSession && pk.end_sequence() == 99 && b[18] == 0xFF && b[19] == 0xFF);
    uint8_t padded[24]; std::memcpy(padded, b, kHeaderSize); CHECK(parse_packet(padded, 21, pk) == ParseError::TrailingBytes);   // payload on EOS
    build_heartbeat(padded, s, 1); CHECK(parse_packet(padded, 21, pk) == ParseError::TrailingBytes); }

  { // request packet = header layout
    uint8_t b[32]; CHECK(build_request(b, s, 500, 1000) == 20);
    CHECK(itch::read_be64(b + 10) == 500 && itch::read_be16(b + 18) == 1000); }

  { // whole-packet validation
    Packet pk; auto good = packet(s, 1, {{1, 2}, {3}});
    CHECK(parse_packet(good.data(), good.size(), pk) == ParseError::None);
    CHECK(parse_packet(good.data(), 0, pk) == ParseError::TooShort && parse_packet(nullptr, 30, pk) == ParseError::TooShort && parse_packet(good.data(), 19, pk) == ParseError::TooShort);
    CHECK(parse_packet(good.data(), good.size() - 1, pk) == ParseError::Truncated);          // last block cut short
    CHECK(parse_packet(good.data(), 21, pk) == ParseError::MissingBlocks);                   // 1 byte of a block header
    auto extra = good; extra.push_back(0); CHECK(parse_packet(extra.data(), extra.size(), pk) == ParseError::TrailingBytes);
    auto lies = good; lies[19] = 3; CHECK(parse_packet(lies.data(), lies.size(), pk) == ParseError::MissingBlocks);      // count says 3, two present
    auto few = good; few[19] = 1; CHECK(parse_packet(few.data(), few.size(), pk) == ParseError::TrailingBytes);         // count says 1, two present
    auto huge = good; huge[20] = 0xFF; huge[21] = 0xFF; CHECK(parse_packet(huge.data(), huge.size(), pk) == ParseError::Truncated);   // block length beyond the packet
    // a 64 KB - 1 message is legal in principle
    std::vector<uint8_t> big(65535, 7); auto bp = packet(s, 1, {big}); CHECK(parse_packet(bp.data(), bp.size(), pk) == ParseError::None); }

  { // builder honours the payload budget and sequence bookkeeping
    uint8_t buf[2048]; PacketBuilder pb(buf, sizeof buf, 64); pb.begin(s, 100);
    const std::vector<uint8_t> m(20, 9);
    CHECK(pb.add(m.data(), 20) && pb.add(m.data(), 20) && !pb.add(m.data(), 20));            // 20 + 22 + 22 = 64 bytes: a third does not fit
    CHECK(pb.count() == 2 && pb.next_sequence() == 102 && !pb.empty());
    const size_t n = pb.finish(); CHECK(n == 20 + 2 * 22);
    Packet pk; CHECK(parse_packet(buf, n, pk) == ParseError::None && pk.first_sequence() == 100 && pk.message_count() == 2);
    pb.begin(s, 7); const std::vector<uint8_t> huge(100, 1); CHECK(!pb.add(huge.data(), 100) && pb.empty());   // can never fit
    CHECK(pb.add(nullptr, 0) && pb.count() == 1 && pb.next_sequence() == 8); }                          // zero-length message

  { // fuzz: arbitrary bytes and mutated valid packets never crash and never expose an out-of-bounds block
    std::mt19937_64 g(1); auto base = packet(s, 77, {{1, 2, 3}, {4}, {}, std::vector<uint8_t>(40, 5)}); size_t ok = 0;
    for (int i = 0; i < 400000; ++i) {
      std::vector<uint8_t> m;
      if (i & 1) { m = base; for (int k = 0, nk = 1 + (int)(g() % 5); k < nk; ++k) m[g() % m.size()] = (uint8_t)g(); if (g() % 3 == 0) m.resize(g() % m.size()); }
      else { m.resize(g() % 80); for (auto &b : m) b = (uint8_t)g(); }
      Packet pk;
      if (parse_packet(m.data(), m.size(), pk) == ParseError::None) {
        ++ok; BlockIterator it(pk); const uint8_t *d; uint16_t n; size_t total = 0, blocks = 0;
        while (it.next(d, n)) { total += n + 2; ++blocks; if (n) CHECK(d >= m.data() + kHeaderSize && d + n <= m.data() + m.size()); }
        CHECK(blocks == pk.message_count() && total == pk.payload_len);
      }
    }
    std::printf("  fuzz: 400000 inputs, %zu accepted, every accepted packet fully accounted for\n", ok); }
  std::puts("ok");
  return 0;
}

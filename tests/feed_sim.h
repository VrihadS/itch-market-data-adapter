#pragma once
// Test support for FeedSession: recording sink / re-request sender, message generation, and a
// deterministic discrete-event simulation of two lossy lines plus a re-request server.
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <queue>
#include <random>
#include <vector>

#include "check.h"
#include "itch/feed/feed_session.hpp"
#include "itch/feed/mold_sender.hpp"

namespace fsim {

using namespace feed_handler;
using namespace feed_handler::feed;
using Bytes = std::vector<uint8_t>;

constexpr uint64_t kMs = 1'000'000ull;

// Message number i: 8-byte big-endian index followed by deterministic filler, so content can be checked.
inline Bytes make_message(uint64_t index, std::mt19937_64 &rng, size_t min_len = 9, size_t max_len = 60) {
  const size_t n = min_len + rng() % (max_len - min_len + 1);
  Bytes b(n);
  itch::write_be64(b.data(), index);
  for (size_t i = 8; i < n; ++i) b[i] = (uint8_t)rng();
  return b;
}

struct RecSink : IFeedSink {
  enum class K { Start, Msg, Caught, End, Stale };
  struct Ev { K k; uint64_t seq = 0; Bytes data; bool catching_up = false, rebuild = false; mold::Session session; };
  std::vector<Ev> ev;
  size_t accept_messages = SIZE_MAX;   // 0 => reject; counts down
  bool accept_control = true;

  static Ev mk(K k, uint64_t seq) { Ev e; e.k = k; e.seq = seq; return e; }
  bool on_session_start(const mold::Session &s, uint64_t first, bool cu, bool rb) override {
    if (!accept_control) return false; Ev e = mk(K::Start, first); e.catching_up = cu; e.rebuild = rb; e.session = s; ev.push_back(e); return true; }
  bool on_message(uint64_t seq, const uint8_t *d, uint16_t n) override {
    if (accept_messages == 0) return false; if (accept_messages != SIZE_MAX) --accept_messages;
    Ev e = mk(K::Msg, seq); e.data.assign(d, d + n); ev.push_back(std::move(e)); return true; }
  bool on_caught_up(uint64_t next) override { if (!accept_control) return false; ev.push_back(mk(K::Caught, next)); return true; }
  bool on_end_of_session(const mold::Session &s, uint64_t next) override { if (!accept_control) return false; Ev e = mk(K::End, next); e.session = s; ev.push_back(e); return true; }
  bool on_stale(StaleReason, uint64_t expected, uint64_t) override { if (!accept_control) return false; ev.push_back(mk(K::Stale, expected)); return true; }

  size_t count(K k) const { size_t n = 0; for (auto &e : ev) n += e.k == k; return n; }
  std::vector<uint64_t> seqs() const { std::vector<uint64_t> v; for (auto &e : ev) if (e.k == K::Msg) v.push_back(e.seq); return v; }
  // Messages delivered since the last session start.
  std::vector<const Ev *> messages_of_last_session() const {
    std::vector<const Ev *> v; for (auto &e : ev) { if (e.k == K::Start) v.clear(); else if (e.k == K::Msg) v.push_back(&e); } return v; }
};

struct RecRerequest : IRerequestSender {
  struct Req { mold::Session s; uint64_t first; uint16_t count; };
  std::vector<Req> reqs;
  bool fail = false;
  bool send_request(const mold::Session &s, uint64_t first, uint16_t count) override {
    if (fail) return false; reqs.push_back({s, first, count}); return true; }
};

// Packets for messages [from, to) of `msgs`, cut to `max_bytes`.
inline std::vector<Bytes> packetize(const mold::Session &s, const std::vector<Bytes> &msgs, size_t from, size_t to, size_t max_bytes) {
  // sequence of msgs[i] is i + 1; build with PacketBuilder so the first sequence is right even for a partial range
  std::vector<Bytes> out; Bytes buf(max_bytes + 64); mold::PacketBuilder pb(buf.data(), buf.size(), max_bytes); pb.begin(s, from + 1);
  for (size_t i = from; i < to; ++i) {
    if (!pb.add(msgs[i].data(), msgs[i].size())) { out.emplace_back(buf.data(), buf.data() + pb.finish()); pb.begin(s, i + 1); CHECK(pb.add(msgs[i].data(), msgs[i].size())); }
  }
  if (!pb.empty()) out.emplace_back(buf.data(), buf.data() + pb.finish());
  return out;
}

inline Bytes heartbeat(const mold::Session &s, uint64_t next) { Bytes b(mold::kHeaderSize); mold::build_heartbeat(b.data(), s, next); return b; }
inline Bytes end_of_session(const mold::Session &s, uint64_t next) { Bytes b(mold::kHeaderSize); mold::build_end_of_session(b.data(), s, next); return b; }

// ── discrete-event simulation ────────────────────────────────────────────────────
struct SimConfig {
  size_t messages = 2000;
  size_t max_packet_bytes = 400;
  uint64_t send_interval_ns = 20'000;       // one packet every 20 us
  uint64_t heartbeat_interval_ns = 20 * kMs;
  uint64_t tail_ns = 400 * kMs;             // end-of-session packets are repeated for this long
  FaultConfig a, b;                         // faults on the two lines
  double reply_loss = 0.0;                  // fraction of re-request replies dropped
  double reply_corrupt = 0.0;
  uint64_t rtt_ns = 300'000, rtt_jitter_ns = 200'000;
  size_t reply_packet_bytes = 400;          // how much one reply can hold
  uint64_t join_after_ns = 0;               // receiver starts listening this late (late join)
  bool end_session = true;
  uint64_t seed = 1;
  uint64_t time_limit_ns = 120'000'000'000ull;
  uint64_t tick_ns = 500'000;
  mold::Session session = mold::Session("SESSION001");
  size_t trim_log_before = 0;               // re-request server retention: drop messages with index < this
};

struct Sim {
  SimConfig cfg;
  FeedConfig fc;
  RecSink sink;
  RecRerequest rr;
  FeedSession session;
  MessageLog log;
  std::vector<Bytes> msgs;
  std::mt19937_64 rng;
  FaultInjector fa, fb;
  uint64_t sent_limit = 1;   // next sequence the sender has put on the wire so far
  uint64_t now = 0, order = 0;
  struct Ev { uint64_t t, o; int type; int line; Bytes bytes; bool operator>(const Ev &x) const { return t != x.t ? t > x.t : o > x.o; } };
  std::priority_queue<Ev, std::vector<Ev>, std::greater<Ev>> q;
  static constexpr int SEND = 0, DELIVER = 1, TICK = 2;

  Sim(const SimConfig &c, const FeedConfig &f) : cfg(c), fc(f), session(f, sink, rr), rng(c.seed * 7919 + 1), fa(c.a, c.seed * 31 + 1), fb(c.b, c.seed * 31 + 2) {
    for (size_t i = 0; i < cfg.messages; ++i) { msgs.push_back(make_message(i + 1, rng)); log.append(msgs.back().data(), msgs.back().size()); }
    if (cfg.trim_log_before) log.trim_before(cfg.trim_log_before + 1);
    // sender schedule
    uint64_t t = 0; auto pk = packetize(cfg.session, msgs, 0, msgs.size(), cfg.max_packet_bytes);
    for (auto &p : pk) { push(t, SEND, 0, p); t += cfg.send_interval_ns; }
    const uint64_t end_t = t;
    const uint64_t hb_until = cfg.end_session ? end_t : end_t + cfg.tail_ns;   // end-of-session packets replace heartbeats
    for (uint64_t h = cfg.heartbeat_interval_ns; h < hb_until; h += cfg.heartbeat_interval_ns)
      push(h, SEND, 1, {});                                                    // contents filled in at send time
    if (cfg.end_session) for (uint64_t e = end_t; e < end_t + cfg.tail_ns; e += cfg.heartbeat_interval_ns) push(e + 1, SEND, 2, Bytes{});
    push(0, TICK, 0, {});                      // ticks re-arm themselves while the session still has work to do
  }
  void push(uint64_t t, int type, int line, Bytes b) { q.push({t, order++, type, line, std::move(b)}); }

  void respond(const RecRerequest::Req &r, uint64_t t) {
    if (std::uniform_real_distribution<double>(0, 1)(rng) < cfg.reply_loss) return;
    Bytes pkt = log.build_packet(cfg.session, r.first, r.count, cfg.reply_packet_bytes, sent_limit);
    if (pkt.empty()) return;
    if (cfg.reply_corrupt > 0 && std::uniform_real_distribution<double>(0, 1)(rng) < cfg.reply_corrupt) pkt.resize(rng() % pkt.size());
    const uint64_t due = t + cfg.rtt_ns + (cfg.rtt_jitter_ns ? rng() % cfg.rtt_jitter_ns : 0);
    push(due, DELIVER, (int)Line::Rerequest, pkt);
  }

  void run() {
    while (!q.empty() && now <= cfg.time_limit_ns) {
      Ev e = q.top(); q.pop(); now = e.t;
      if (e.type == SEND) {
        Bytes pkt = e.bytes;
        if (e.line == 1) pkt = heartbeat(cfg.session, sent_limit);
        if (e.line == 2) pkt = end_of_session(cfg.session, (uint64_t)cfg.messages + 1);
        if (e.line == 0) { mold::Packet p; CHECK(mold::parse_packet(pkt.data(), pkt.size(), p) == mold::ParseError::None); sent_limit = std::max(sent_limit, p.end_sequence()); }
        std::vector<FaultInjector::Delivery> out;
        fa.apply(pkt.data(), pkt.size(), now, out); for (auto &d : out) push(d.due_ns, DELIVER, (int)Line::A, std::move(d.bytes));
        out.clear(); fb.apply(pkt.data(), pkt.size(), now, out); for (auto &d : out) push(d.due_ns, DELIVER, (int)Line::B, std::move(d.bytes));
      } else if (e.type == DELIVER) {
        if (now >= cfg.join_after_ns || e.line == (int)Line::Rerequest) {
          const size_t before = rr.reqs.size();
          session.on_packet((Line)e.line, e.bytes.data(), e.bytes.size(), now);
          for (size_t i = before; i < rr.reqs.size(); ++i) respond(rr.reqs[i], now);
        }
      } else {
        const size_t before = rr.reqs.size();
        if (now >= cfg.join_after_ns) session.tick(now);
        for (size_t i = before; i < rr.reqs.size(); ++i) respond(rr.reqs[i], now);
        // keep ticking until the session is over (late duplicates still arrive and must be ignored), or give up
        const bool finished = session.state() == State::Ended || session.state() == State::Stale;
        if (!(finished && q.empty()) && now + cfg.tick_ns <= cfg.time_limit_ns) push(now + cfg.tick_ns, TICK, 0, {});
      }
    }
  }

  // The delivered stream must be exactly messages [from_index, end) of the original, in order, once each.
  void check_stream(size_t from_seq = 1) const {
    auto got = sink.messages_of_last_session();
    CHECK(got.size() == msgs.size() - (from_seq - 1));
    for (size_t i = 0; i < got.size(); ++i) {
      CHECK(got[i]->seq == from_seq + i);
      CHECK(got[i]->data == msgs[from_seq - 1 + i]);
    }
  }
};

}  // namespace fsim

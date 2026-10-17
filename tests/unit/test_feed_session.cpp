// FeedSession scenarios, one behavior each, with exact expectations.
#include <algorithm>

#include "feed_sim.h"

using namespace fsim;
using K = RecSink::K;

struct Rig {
  FeedConfig cfg; RecSink sink; RecRerequest rr; MessageLog log; std::vector<Bytes> msgs; std::mt19937_64 rng{42};
  mold::Session s{"SESSION001"}; std::unique_ptr<FeedSession> fs; uint64_t sent_limit = UINT64_MAX; size_t served = 0;
  explicit Rig(size_t n, FeedConfig c = defaults()) : cfg(c) {
    for (size_t i = 0; i < n; ++i) { msgs.push_back(make_message(i + 1, rng)); log.append(msgs.back().data(), msgs.back().size()); }
    fs = std::make_unique<FeedSession>(cfg, sink, rr);
  }
  static FeedConfig defaults() { FeedConfig c; c.gap_wait_ns = 5 * kMs; c.request_timeout_ns = 50 * kMs; c.max_request_attempts = 3; return c; }
  std::vector<Bytes> packets(size_t from, size_t to, size_t max_bytes = 300) const { return packetize(s, msgs, from, to, max_bytes); }
  void feed(Line l, const Bytes &b, uint64_t t) { fs->on_packet(l, b.data(), b.size(), t); }
  // Answer every request not yet served, immediately, like a re-request server holding the whole log.
  void serve(uint64_t t, size_t reply_bytes = 300) {
    while (served < rr.reqs.size()) {
      const auto r = rr.reqs[served++]; const Bytes pkt = log.build_packet(s, r.first, r.count, reply_bytes, sent_limit);
      if (!pkt.empty()) feed(Line::Rerequest, pkt, t);
    }
  }
  void expect_full_stream(size_t n) const { auto g = sink.messages_of_last_session(); CHECK(g.size() == n); for (size_t i = 0; i < n; ++i) { CHECK(g[i]->seq == i + 1); CHECK(g[i]->data == msgs[i]); } }
};

int main() {
  { // 1. one line, in order, then end of session
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 1\n");
    Rig r(100); uint64_t t = 1 * kMs;
    for (auto &p : r.packets(0, 100)) r.feed(Line::A, p, t += 100'000);
    r.feed(Line::A, end_of_session(r.s, 101), t += kMs);
    r.expect_full_stream(100);
    CHECK(r.fs->state() == State::Ended && r.sink.count(K::End) == 1 && r.sink.ev.back().seq == 101);
    auto st = r.fs->stats(); CHECK(st.line[0].messages_won == 100 && st.line[1].packets == 0 && st.requests_sent == 0 && st.gaps_detected == 0);
    CHECK(r.sink.ev.front().k == K::Start && r.sink.ev.front().seq == 1 && !r.sink.ev.front().catching_up);
  }
  { // 2. both lines healthy: every message delivered once, the second copy counted as a duplicate
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 2\n");
    Rig r(100); uint64_t t = 1 * kMs; auto pk = r.packets(0, 100);
    for (auto &p : pk) { r.feed(Line::A, p, t); r.feed(Line::B, p, t + 200'000); t += 400'000; }
    r.expect_full_stream(100);
    auto st = r.fs->stats(); CHECK(st.line[0].messages_won == 100 && st.line[1].messages_won == 0 && st.line[1].duplicate_packets == pk.size());
    CHECK(st.requests_sent == 0 && r.fs->line_up(Line::A) && r.fs->line_up(Line::B));
  }
  { // 3. A loses a packet, B has it a moment later: arbitration fills the gap, the server is never asked
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 3\n");
    Rig r(100); uint64_t t = 1 * kMs; auto pk = r.packets(0, 100); const size_t lost = 3;
    for (size_t i = 0; i < pk.size(); ++i) { if (i != lost) r.feed(Line::A, pk[i], t); r.feed(Line::B, pk[i], t + 1 * kMs); t += 2 * kMs; r.fs->tick(t); }
    r.expect_full_stream(100);
    auto st = r.fs->stats(); CHECK(st.requests_sent == 0 && st.line[1].messages_won > 0 && st.gaps_closed == st.gaps_detected);
  }
  { // 4. both lines lose the same packet: after gap_wait the server is asked for exactly the missing range
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 4\n");
    Rig r(100); uint64_t t = 1 * kMs; auto pk = r.packets(0, 100); const size_t lost = 3;
    mold::Packet lp; CHECK(mold::parse_packet(pk[lost].data(), pk[lost].size(), lp) == mold::ParseError::None);
    for (size_t i = 0; i < pk.size(); ++i) if (i != lost) { r.feed(Line::A, pk[i], t); r.feed(Line::B, pk[i], t + 5'000); t += 10'000; }
    CHECK(r.rr.reqs.empty());                                   // the whole burst fits inside the 5 ms grace period
    r.fs->tick(t + 6 * kMs);                                    // grace over
    CHECK(r.rr.reqs.size() == 1 && r.rr.reqs[0].first == lp.first_sequence() && r.rr.reqs[0].count == lp.message_count());
    CHECK(r.rr.reqs[0].s == r.s);
    r.serve(t + 7 * kMs);
    r.feed(Line::A, end_of_session(r.s, 101), t + 8 * kMs);
    r.expect_full_stream(100);
    auto st = r.fs->stats(); CHECK(st.recovered_messages == lp.message_count() && st.requests_sent == 1 && r.fs->state() == State::Ended);
  }
  { // 5. reordering inside the grace period needs no request
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 5\n");
    Rig r(100); uint64_t t = 1 * kMs; auto pk = r.packets(0, 100);
    r.feed(Line::A, pk[0], t);                                  // the first packet defines the join point, so it arrives in order
    for (size_t i = 1; i + 1 < pk.size(); i += 2) { r.feed(Line::A, pk[i + 1], t += 3 * kMs); r.feed(Line::A, pk[i], t + 1 * kMs); r.fs->tick(t += 3 * kMs); }
    if (pk.size() % 2 == 0) r.feed(Line::A, pk.back(), t += 3 * kMs);
    r.expect_full_stream(100); CHECK(r.rr.reqs.empty());
  }
  { // 6. late join: first packet is mid-stream; the session replays from 1 via re-requests, tells the sink it is catching up
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 6\n");
    Rig r(300); auto pk = r.packets(0, 300); const size_t join_at = pk.size() / 2; uint64_t t = 1 * kMs; size_t reqs_seen = 0;
    r.feed(Line::A, pk[join_at], t);
    CHECK(r.sink.ev.size() == 1 && r.sink.ev[0].k == K::Start && r.sink.ev[0].seq == 1 && r.sink.ev[0].catching_up);
    CHECK(r.fs->state() == State::CatchingUp && !r.rr.reqs.empty() && r.rr.reqs.size() <= r.cfg.request_window && r.rr.reqs[0].first == 1);   // no grace wait; several requests ride in a window
    for (size_t i = join_at + 1; i < pk.size(); ++i) { r.feed(Line::A, pk[i], t += 100'000); }
    for (int guard = 0; guard < 2000 && r.fs->state() == State::CatchingUp; ++guard) { r.serve(t += 100'000); r.fs->tick(t); }
    CHECK(r.fs->state() == State::Live && r.sink.count(K::Caught) == 1);
    r.feed(Line::A, end_of_session(r.s, 301), t += kMs); r.fs->tick(t += kMs);
    r.expect_full_stream(300); CHECK(r.fs->state() == State::Ended); (void)reqs_seen;
    // the caught-up marker comes after message 300 or earlier but strictly before the end marker
    size_t caught = 0, endi = 0; for (size_t i = 0; i < r.sink.ev.size(); ++i) { if (r.sink.ev[i].k == K::Caught) caught = i; if (r.sink.ev[i].k == K::End) endi = i; }
    CHECK(caught > 0 && caught < endi);
  }
  { // 7. live-tail only: no replay, no requests
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 7\n");
    FeedConfig c = Rig::defaults(); c.join_from_start = false; Rig r(100, c); auto pk = r.packets(0, 100); uint64_t t = kMs;
    for (size_t i = 5; i < pk.size(); ++i) r.feed(Line::A, pk[i], t += 100'000);
    mold::Packet p5; CHECK(mold::parse_packet(pk[5].data(), pk[5].size(), p5) == mold::ParseError::None);
    CHECK(r.sink.ev[0].k == K::Start && r.sink.ev[0].seq == p5.first_sequence() && !r.sink.ev[0].catching_up);
    CHECK(r.rr.reqs.empty() && r.fs->state() == State::Live && r.sink.seqs().front() == p5.first_sequence() && r.sink.seqs().back() == 100);
  }
  { // 8. the last packet is lost on both lines; a heartbeat reveals it, then end of session completes the session
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 8\n");
    Rig r(100); auto pk = r.packets(0, 100); uint64_t t = kMs;
    for (size_t i = 0; i + 1 < pk.size(); ++i) { r.feed(Line::A, pk[i], t += 100'000); r.feed(Line::B, pk[i], t += 10'000); }
    r.feed(Line::A, heartbeat(r.s, 101), t += kMs); r.feed(Line::B, heartbeat(r.s, 101), t);
    CHECK(r.rr.reqs.empty() && r.fs->state() == State::Live);
    r.fs->tick(t += 6 * kMs); CHECK(r.rr.reqs.size() == 1);
    r.serve(t += kMs); r.feed(Line::A, end_of_session(r.s, 101), t += kMs);
    r.expect_full_stream(100); CHECK(r.fs->state() == State::Ended);
  }
  { // 9. end of session while messages are still missing: not Ended until they are recovered
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 9\n");
    Rig r(100); auto pk = r.packets(0, 100); uint64_t t = kMs;
    for (size_t i = 0; i + 1 < pk.size(); ++i) r.feed(Line::A, pk[i], t += 100'000);
    r.feed(Line::A, end_of_session(r.s, 101), t += kMs);
    CHECK(r.fs->state() == State::Live && r.sink.count(K::End) == 0);
    r.fs->tick(t += 6 * kMs); r.serve(t += kMs); r.fs->tick(t += kMs);
    CHECK(r.fs->state() == State::Ended && r.sink.count(K::End) == 1); r.expect_full_stream(100);
    const size_t n = r.sink.ev.size(); r.feed(Line::A, pk[0], t += kMs); r.feed(Line::A, end_of_session(r.s, 101), t += kMs); CHECK(r.sink.ev.size() == n);   // nothing more after the end
  }
  { // 10. a new session after the old one ended starts clean at sequence 1
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 10\n");
    Rig r(60); auto pk = r.packets(0, 60); uint64_t t = kMs;
    for (auto &p : pk) r.feed(Line::A, p, t += 100'000);
    r.feed(Line::A, end_of_session(r.s, 61), t += kMs); CHECK(r.fs->state() == State::Ended);
    mold::Session s2("SESSION002"); auto pk2 = packetize(s2, r.msgs, 0, 30, 300);
    for (auto &p : pk2) r.feed(Line::A, p, t += 100'000);
    CHECK(r.sink.count(K::Start) == 2 && r.sink.ev.back().k == K::Msg && r.fs->state() == State::Live && r.fs->session() == s2);
    CHECK(r.sink.messages_of_last_session().size() == 30 && r.fs->stats().session_starts == 2);
  }
  { // 11. a stray packet from another session is ignored; a silent session is eventually replaced
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 11\n");
    Rig r(60); auto pk = r.packets(0, 30); uint64_t t = kMs; for (auto &p : pk) r.feed(Line::A, p, t += 100'000);
    mold::Session other("OTHER00001"); auto op = packetize(other, r.msgs, 0, 10, 300);
    for (auto &p : op) r.feed(Line::B, p, t += 100'000);
    CHECK(r.fs->stats().session_mismatch_packets == op.size() && r.fs->session() == mold::Session("SESSION001") && r.sink.count(K::Start) == 1);
    t += 6'000 * kMs; r.feed(Line::B, op[0], t);                       // the old session has been silent for > 5 s
    CHECK(r.sink.count(K::Start) == 2 && r.fs->session() == other);
  }
  { // 12. the server never answers: Stale, nothing after the gap is delivered; rebuild() replays from 1
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 12\n");
    Rig r(100); auto pk = r.packets(0, 100); uint64_t t = kMs; const size_t lost = 2;
    for (size_t i = 0; i < pk.size(); ++i) if (i != lost) r.feed(Line::A, pk[i], t += 100'000);
    mold::Packet lp; CHECK(mold::parse_packet(pk[lost].data(), pk[lost].size(), lp) == mold::ParseError::None);
    for (int i = 0; i < 200 && r.fs->state() == State::Live; ++i) r.fs->tick(t += 10 * kMs);
    CHECK(r.fs->state() == State::Stale && r.sink.count(K::Stale) == 1 && r.rr.reqs.size() == 3);   // max_request_attempts requests, then give up
    CHECK(r.sink.ev.back().k == K::Stale && r.sink.ev.back().seq == lp.first_sequence());
    CHECK(r.sink.seqs().back() == lp.first_sequence() - 1);                                          // everything after the gap withheld
    const size_t n = r.sink.ev.size(); r.feed(Line::A, pk[10], t += kMs); r.fs->tick(t += kMs); CHECK(r.sink.ev.size() == n);   // and it stays withheld
    r.fs->rebuild(t += kMs);
    CHECK(r.sink.ev.back().k == K::Start && r.sink.ev.back().rebuild && r.sink.ev.back().seq == 1 && r.sink.ev.back().catching_up);
    CHECK(r.fs->state() == State::CatchingUp);
    r.feed(Line::A, heartbeat(r.s, 101), t += kMs);                                                   // the feed's position
    for (int guard = 0; guard < 500 && r.fs->state() == State::CatchingUp; ++guard) { r.serve(t += 100'000); r.fs->tick(t += 100'000); }
    CHECK(r.fs->state() == State::Live); r.expect_full_stream(100); CHECK(r.fs->stats().rebuilds == 1);
  }
  { // 13. a lost reply is retried and recovery still succeeds (no Stale)
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 13\n");
    Rig r(100); auto pk = r.packets(0, 100); uint64_t t = kMs;
    for (size_t i = 0; i < pk.size(); ++i) if (i != 4) r.feed(Line::A, pk[i], t += 100'000);
    r.fs->tick(t += 6 * kMs); CHECK(r.rr.reqs.size() == 1);
    r.fs->tick(t += 60 * kMs); CHECK(r.rr.reqs.size() == 2 && r.rr.reqs[1].first == r.rr.reqs[0].first && r.fs->stats().request_retries == 1);
    r.served = 1; r.serve(t += kMs);                                       // only the retry is answered
    r.feed(Line::A, end_of_session(r.s, 101), t += kMs); r.expect_full_stream(100); CHECK(r.fs->state() == State::Ended && r.fs->stats().stale_events == 0);
  }
  { // 14. the server returns only part of what was asked: the next request follows at once, without waiting for a timeout
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 14\n");
    Rig r(200); auto pk = r.packets(0, 200); uint64_t t = kMs;
    for (size_t i = 0; i < pk.size(); ++i) if (i < 2 || i > 20) r.feed(Line::A, pk[i], t += 100'000);      // a wide hole
    r.fs->tick(t += 6 * kMs); const uint64_t t_hole = t;
    for (int guard = 0; guard < 200 && r.fs->state() == State::Live && r.fs->next_sequence() < 200; ++guard) { r.serve(t += 50'000, /*reply_bytes=*/80); if (r.rr.reqs.size() == r.served && r.fs->stats().gaps_closed) break; }
    r.feed(Line::A, end_of_session(r.s, 201), t += kMs); r.expect_full_stream(200);
    CHECK(r.rr.reqs.size() > 3 && t - t_hole < 40 * kMs && r.fs->stats().request_retries == 0);        // many requests, no timeout waits
  }
  { // 15. malformed and absurd packets are counted and change nothing
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 15\n");
    Rig r(50); auto pk = r.packets(0, 50); uint64_t t = kMs; r.feed(Line::A, pk[0], t);
    const size_t n = r.sink.ev.size(); Bytes cut(pk[1].begin(), pk[1].end() - 1); r.feed(Line::A, cut, t += kMs);
    Bytes junk(7, 0xAB); r.feed(Line::B, junk, t += kMs); r.feed(Line::B, Bytes{}, t);
    Bytes zero(20, 0); std::memcpy(zero.data(), "SESSION001", 10); zero[19] = 1; zero[20 - 1] = 1; r.feed(Line::A, Bytes(zero.begin(), zero.end()), t);   // sequence 0 with a count: invalid
    Bytes huge = heartbeat(r.s, 1ull << 60); r.feed(Line::A, huge, t += kMs);
    auto st = r.fs->stats(); CHECK(r.sink.ev.size() == n && st.line[0].malformed >= 1 && st.line[1].malformed == 2 && st.out_of_range_packets >= 1);
    CHECK(r.fs->state() == State::Live && st.highest_seen < (1ull << 40));
  }
  { // 16. a zero-length message is legal and still consumes its sequence number
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 16\n");
    Rig r(0); std::vector<Bytes> m = {Bytes{1, 2, 3}, Bytes{}, Bytes{4}, Bytes{}, Bytes{}, Bytes{5, 6}}; r.msgs = m;
    for (auto &x : m) r.log.append(x.data(), x.size());
    auto pk = packetize(r.s, m, 0, m.size(), 300); CHECK(pk.size() == 1); r.feed(Line::A, pk[0], kMs);
    CHECK((r.sink.seqs() == std::vector<uint64_t>{1, 3, 6}) && r.fs->next_sequence() == 7 && r.fs->stats().zero_length == 3 && r.fs->stats().gaps_detected == 0);
    r.feed(Line::A, end_of_session(r.s, 7), 2 * kMs); CHECK(r.fs->state() == State::Ended);
  }
  { // 17. the sink is full: nothing is lost or reordered, and the server is NOT asked to resend what we already hold
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 17\n");
    Rig r(200); auto pk = r.packets(0, 200); uint64_t t = kMs; r.sink.accept_messages = 5;
    for (auto &p : pk) r.feed(Line::A, p, t += 100'000);
    for (int i = 0; i < 50; ++i) r.fs->tick(t += 20 * kMs);
    CHECK(r.sink.seqs().size() == 5 && r.rr.reqs.empty() && r.fs->stats().sink_blocked > 0);
    r.sink.accept_messages = SIZE_MAX; r.fs->tick(t += kMs);
    r.feed(Line::A, end_of_session(r.s, 201), t += kMs); r.expect_full_stream(200);
    CHECK(r.fs->state() == State::Ended && r.rr.reqs.empty());
  }
  { // 18. control events keep their order relative to data even when the sink is full for them
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 18\n");
    Rig r(30); auto pk = r.packets(0, 30); uint64_t t = kMs; r.sink.accept_control = false;
    for (auto &p : pk) r.feed(Line::A, p, t += 100'000);
    CHECK(r.sink.ev.empty());                                                        // no data before the session-start marker has been accepted
    r.sink.accept_control = true; r.fs->tick(t += kMs);
    CHECK(r.sink.ev.size() == 31 && r.sink.ev[0].k == K::Start); r.expect_full_stream(30);
  }
  { // 19. a tiny reorder buffer drops far-ahead packets; the server fills the holes; the stream is still exact
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 19\n");
    FeedConfig c = Rig::defaults(); c.max_buffered_packets = 3; Rig r(200, c); auto pk = r.packets(0, 200); uint64_t t = kMs;
    for (size_t i = pk.size(); i-- > 0;) r.feed(Line::A, pk[i], t += 50'000);        // arrives fully reversed
    for (int guard = 0; guard < 5000 && r.fs->next_sequence() <= 200; ++guard) { r.fs->tick(t += kMs); r.serve(t += 100'000); }
    r.feed(Line::A, end_of_session(r.s, 201), t += kMs); r.expect_full_stream(200);
    CHECK(r.fs->stats().buffer_drops > 0 && r.fs->state() == State::Ended);
  }
  { // 20. line liveness
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 20\n");
    Rig r(40); auto pk = r.packets(0, 40); uint64_t t = kMs;
    r.feed(Line::A, pk[0], t); r.feed(Line::B, pk[0], t); CHECK(r.fs->line_up(Line::A) && r.fs->line_up(Line::B));
    for (int i = 0; i < 20; ++i) { t += 500 * kMs; r.feed(Line::B, heartbeat(r.s, 1), t); r.fs->tick(t); }
    CHECK(!r.fs->line_up(Line::A) && r.fs->line_up(Line::B) && r.fs->stats().line[0].outages == 1);
    r.feed(Line::A, heartbeat(r.s, 1), t += kMs); CHECK(r.fs->line_up(Line::A));
  }
  { // 21. after Stale, auto-rebuild kicks in on its own
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 21\n");
    FeedConfig c = Rig::defaults(); c.auto_rebuild_after_ns = 200 * kMs; Rig r(60, c); auto pk = r.packets(0, 60); uint64_t t = kMs;
    for (size_t i = 0; i < pk.size(); ++i) if (i != 1) r.feed(Line::A, pk[i], t += 100'000);
    for (int i = 0; i < 100 && r.fs->state() == State::Live; ++i) r.fs->tick(t += 10 * kMs);
    CHECK(r.fs->state() == State::Stale); const size_t reqs = r.rr.reqs.size(); const uint64_t stale_at = t;
    for (int i = 0; i < 100 && r.fs->stats().rebuilds == 0; ++i) r.fs->tick(t += 10 * kMs);
    CHECK(r.fs->stats().rebuilds == 1 && t - stale_at >= 200 * kMs && t - stale_at < 260 * kMs);       // after auto_rebuild_after_ns, not before
    CHECK(r.sink.ev.back().k == K::Start && r.sink.ev.back().rebuild && r.fs->state() == State::CatchingUp && r.rr.reqs.size() > reqs);
  }
  { // 22. overlapping retransmission: only the new messages are delivered
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 22\n");
    Rig r(100); uint64_t t = kMs; auto pk = r.packets(0, 20); for (auto &p : pk) r.feed(Line::A, p, t += 100'000);
    const uint64_t have = r.fs->next_sequence() - 1;
    Bytes overlap = r.log.build_packet(r.s, have - 5, 30, 600); r.feed(Line::Rerequest, overlap, t += kMs);
    auto got = r.sink.seqs(); for (size_t i = 0; i < got.size(); ++i) CHECK(got[i] == i + 1);          // contiguous, nothing twice
    CHECK(got.size() > have);
  }
  { // 23. a failed request send is retried on the next tick
    if (getenv("TRACE")) std::fprintf(stderr, "scenario 23\n");
    Rig r(60); auto pk = r.packets(0, 60); uint64_t t = kMs; r.rr.fail = true;
    for (size_t i = 0; i < pk.size(); ++i) if (i != 1) r.feed(Line::A, pk[i], t += 100'000);
    r.fs->tick(t += 6 * kMs); CHECK(r.rr.reqs.empty() && r.fs->stats().request_send_failures >= 1);
    r.rr.fail = false; r.fs->tick(t += kMs); CHECK(r.rr.reqs.size() == 1);
  }
  std::puts("ok");
  return 0;
}

// Randomized end-to-end check of FeedSession: random message streams, random packetization, random
// faults on both lines and on the re-request path, random late joins. Whatever happens, the sink must see
// exactly the original stream, once, in order, and the session must end cleanly.
#include <cstdlib>

#include "feed_sim.h"

using namespace fsim;
using K = RecSink::K;

static FaultConfig light(std::mt19937_64 &g) {
  auto r = [&](double m) { return std::uniform_real_distribution<double>(0, m)(g); };
  FaultConfig f; f.loss = r(0.10); f.duplicate = r(0.10); f.reorder = r(0.20); f.reorder_delay_ns = (g() % 3000) * 1000;
  f.corrupt = r(0.03); f.base_delay_ns = (g() % 500) * 1000; f.jitter_ns = (g() % 2000) * 1000; return f;
}
static FaultConfig heavy(std::mt19937_64 &g) {
  auto r = [&](double m) { return std::uniform_real_distribution<double>(0, m)(g); };
  FaultConfig f = light(g); f.loss = r(0.45); f.burst_start = r(0.03); f.burst_len = 3 + g() % 23; f.corrupt = r(0.05); return f;
}

int main(int argc, char **argv) {
  const uint64_t seeds = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 300;
  uint64_t totals[8] = {};   // messages, requests, recovered, line-B wins, late joins, rebuilds(0), gaps, duplicates dropped
  size_t by_class[4] = {};
  for (uint64_t seed = 1; seed <= seeds; ++seed) {
    std::mt19937_64 g(seed * 1000003);
    SimConfig c; c.seed = seed;
    c.messages = 200 + g() % 3800;
    c.max_packet_bytes = 100 + g() % 1100;
    c.reply_packet_bytes = 150 + g() % 650;       // a few messages per reply at least (a real reply holds a full UDP packet)
    c.reply_loss = std::uniform_real_distribution<double>(0, 0.35)(g);
    c.reply_corrupt = std::uniform_real_distribution<double>(0, 0.03)(g);
    c.rtt_ns = (100 + g() % 1500) * 1000; c.rtt_jitter_ns = (g() % 1000) * 1000;
    c.tail_ns = 3'000 * kMs; c.time_limit_ns = 600'000 * kMs;
    const int cls = (int)(seed % 4);
    switch (cls) {
      case 0: c.a = light(g); c.b = light(g); break;
      case 1: c.a = heavy(g); c.b = heavy(g); break;
      case 2: c.a = heavy(g); c.b = heavy(g); c.a.loss = c.b.loss = 0.9; break;                       // both lines nearly dead: recovery does the work
      case 3: c.a = heavy(g); c.b = light(g); c.b.loss = 0; c.b.base_delay_ns = (5 + g() % 45) * kMs; break;   // B is slow but complete
    }
    ++by_class[cls];
    const bool late = g() % 100 < 30;
    if (late) c.join_after_ns = (1 + g() % 40) * kMs;

    FeedConfig f;
    f.gap_wait_ns = (1 + g() % 20) * kMs; f.request_timeout_ns = (5 + g() % 95) * kMs; f.max_request_attempts = 60;
    f.max_request_messages = 50 + g() % 950;
    if (g() % 3 == 0) { f.max_buffered_packets = 2 + g() % 9; }

    Sim sim(c, f); sim.run();

    const auto st = sim.session.stats();
    if (sim.session.state() != State::Ended || sim.sink.count(K::Stale) != 0 || sim.sink.count(K::End) != 1) {
      std::fprintf(stderr, "seed %llu class %d: state=%s next=%llu/%zu stale=%zu end=%zu requests=%llu retries=%llu\n", (unsigned long long)seed, cls,
                   to_string(sim.session.state()), (unsigned long long)st.next_sequence, c.messages + 1, sim.sink.count(K::Stale), sim.sink.count(K::End),
                   (unsigned long long)st.requests_sent, (unsigned long long)st.request_retries);
      return 1;
    }
    sim.check_stream(1);
    CHECK(sim.sink.count(K::Start) == 1 && sim.sink.ev.front().k == K::Start && sim.sink.ev.front().seq == 1);
    if (sim.sink.ev.front().catching_up) CHECK(sim.sink.count(K::Caught) == 1 && st.caught_up_events == 1);
    CHECK(st.next_sequence == c.messages + 1);
    totals[0] += c.messages; totals[1] += st.requests_sent; totals[2] += st.recovered_messages; totals[3] += st.line[1].messages_won;
    totals[4] += sim.sink.ev.front().catching_up; totals[6] += st.gaps_detected; totals[7] += st.line[0].duplicate_packets + st.line[1].duplicate_packets;
  }
  std::printf("  %llu runs (%zu/%zu/%zu/%zu by profile): %llu messages delivered exactly once and in order\n", (unsigned long long)seeds,
              by_class[0], by_class[1], by_class[2], by_class[3], (unsigned long long)totals[0]);
  std::printf("  work done: %llu gaps detected, %llu re-requests, %llu messages recovered from the server, %llu won by line B, %llu late joins, %llu duplicate packets dropped\n",
              (unsigned long long)totals[6], (unsigned long long)totals[1], (unsigned long long)totals[2], (unsigned long long)totals[3], (unsigned long long)totals[4], (unsigned long long)totals[7]);
  std::puts("ok");
  return 0;
}

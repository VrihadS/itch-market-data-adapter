// The whole feed path over real UDP sockets on loopback:
//   MoldFeedSender (loss / bursts / duplicates / reordering / delay / corruption on two lines)
//     -> UDPSocket A and B -> FeedSession -> SPSC ring -> FeedConsumer -> BookManager
//   with a UDP re-request server answering gap and catch-up requests.
// Every scenario must end with books identical to applying the same messages directly.
#include <chrono>
#include <thread>
#include <unistd.h>

#include <spdlog/spdlog.h>

#include "check.h"
#include "feed_flow.h"
#include "itch/feed/feed_consumer.hpp"
#include "itch/feed/feed_handler.hpp"
#include "itch/feed/mold_sender.hpp"

using namespace feed_handler;
using namespace feed_handler::feed;
using Clock = std::chrono::steady_clock;

static uint64_t now_ns() { return (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count(); }

struct Scenario {
  const char *name; FaultConfig a, b; double reply_drop = 0; size_t messages = 15000; double join_after = 0; bool no_server = false;
  double rate_msgs_per_s = 150000; size_t reply_bytes = 1400;
};

static int run(const Scenario &sc, unsigned port_base, uint64_t seed, bool verbose) {
  const auto flow = flow::make(sc.messages, seed);
  BookManager ref(1 << 14); NasdaqItchDecoder dec;
  for (auto &m : flow) { size_t u = 0; dec.DecodeMessage((const char *)m.data(), m.size(), u, ref); }
  const uint64_t want_digest = ref.state_digest();

  SocketManager sm; CHECK(sm.Init() == error_code::ok && sm.Start() == error_code::ok);
  const mold::Session session("SESSION001");
  MessageLog log; std::atomic<uint64_t> sent_next{1};
  UdpRerequestServer::Options so; so.drop_probability = sc.reply_drop; so.seed = seed; so.max_packet_bytes = sc.reply_bytes;
  UdpRerequestServer server(session, log, sent_next, so); std::string err;
  CHECK(server.start("127.0.0.1", 0, err));

  UdpSender tx_a, tx_b; CHECK(tx_a.open("127.0.0.1", (uint16_t)(port_base), "", 1, err) && tx_b.open("127.0.0.1", (uint16_t)(port_base + 1), "", 1, err));
  MoldFeedSender::Config mc; mc.session = session; mc.fault_a = sc.a; mc.fault_b = sc.b; mc.seed = seed; mc.heartbeat_interval_ns = 20'000'000; mc.end_of_session_tail_ns = 1'500'000'000ull;
  MoldFeedSender sender(mc, tx_a, tx_b, log, sent_next);

  SpscRing ring(8u << 20); BookManager books(1 << 14); FeedConsumer consumer(ring, books);
  FeedHandlerConfig hc; hc.line_a = {"127.0.0.1", (uint16_t)port_base}; hc.line_b = {"127.0.0.1", (uint16_t)(port_base + 1)};
  if (!sc.no_server) hc.rerequest_server = {"127.0.0.1", server.port()};
  hc.feed.gap_wait_ns = 5'000'000; hc.feed.request_timeout_ns = 40'000'000; hc.feed.max_request_attempts = 40; hc.feed.line_timeout_ns = 500'000'000;
  hc.udp.rcvbuf_bytes = 4 << 20;
  FeedHandler handler(sm, hc, ring);

  const size_t join_at = (size_t)(sc.join_after * flow.size());
  bool started = false;
  if (join_at == 0) { CHECK(handler.Start(err)); started = true; }

  const uint64_t t0 = now_ns(); size_t sent = 0;
  const auto deadline = Clock::now() + std::chrono::seconds(40);
  bool ended = false;
  while (Clock::now() < deadline) {
    const uint64_t t = now_ns();
    const size_t due = std::min<size_t>(flow.size(), (size_t)((t - t0) / 1e9 * sc.rate_msgs_per_s) + 1);
    while (sent < due) {
      if (!started && sent >= join_at) { sender.flush(t); CHECK(handler.Start(err)); started = true; }
      sender.publish(flow[sent].data(), flow[sent].size(), t); ++sent;
    }
    if (sent == flow.size() && !ended) { sender.end_session(t); ended = true; }
    sender.service(t);
    consumer.poll(2048);
    if (ended && consumer.mode() == FeedConsumer::Mode::Ended) break;
    if (sc.no_server && consumer.mode() == FeedConsumer::Mode::Stale) break;
    if (consumer.poll(0) == 0 && sent == flow.size()) std::this_thread::sleep_for(std::chrono::microseconds(200));
  }
  for (int i = 0; i < 50; ++i) { consumer.poll(4096); std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
  const FeedStats st = handler.stats();
  handler.Stop(); server.stop(); sm.Stop();

  if (sc.no_server) {
    CHECK(consumer.mode() == FeedConsumer::Mode::Stale && st.stale_events >= 1);
    std::printf("  %-34s ok  (no re-request server: went Stale as it must; %llu requests, none answered)\n", sc.name, (unsigned long long)st.requests_sent);
    return 0;
  }
  const bool ok = consumer.mode() == FeedConsumer::Mode::Ended && books.state_digest() == want_digest && consumer.decode_failures() == 0;
  if (!ok || verbose)
    std::printf("  %-34s %s mode=%d msgs=%llu/%zu digest=%s next=%llu gaps=%llu req=%llu retry=%llu rec=%llu stale=%llu ringfull=%llu\n", sc.name, ok ? "ok " : "FAIL",
                (int)consumer.mode(), (unsigned long long)consumer.messages(), flow.size(), books.state_digest() == want_digest ? "same" : "DIFFERENT",
                (unsigned long long)st.next_sequence, (unsigned long long)st.gaps_detected, (unsigned long long)st.requests_sent, (unsigned long long)st.request_retries,
                (unsigned long long)st.recovered_messages, (unsigned long long)st.stale_events, (unsigned long long)handler.ring_data_full());
  CHECK(ok);
  CHECK(consumer.messages() == flow.size() && st.stale_events == 0 && handler.ring_data_full() == 0 && books.validate_all());
  std::printf("  %-34s ok  | A: %llu pkts (%llu lost, %llu dup, %llu corrupt) B: %llu pkts (%llu lost) | gaps %llu, requests %llu, recovered %llu msgs, B won %llu, malformed %llu\n",
              sc.name, (unsigned long long)st.line[0].packets, (unsigned long long)sender.dropped(0), (unsigned long long)sender.duplicated(0), (unsigned long long)sender.corrupted(0),
              (unsigned long long)st.line[1].packets, (unsigned long long)sender.dropped(1), (unsigned long long)st.gaps_detected, (unsigned long long)st.requests_sent,
              (unsigned long long)st.recovered_messages, (unsigned long long)st.line[1].messages_won, (unsigned long long)(st.line[0].malformed + st.line[1].malformed));
  return 0;
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::off);
  const unsigned base = 42000 + (unsigned)(::getpid() % 1500) * 16;
  const bool verbose = argc > 1;

  FaultConfig clean;
  FaultConfig mild; mild.loss = 0.05; mild.duplicate = 0.05; mild.reorder = 0.10; mild.reorder_delay_ns = 2'000'000; mild.corrupt = 0.02; mild.jitter_ns = 500'000;
  FaultConfig heavy; heavy.loss = 0.30; heavy.burst_start = 0.01; heavy.burst_len = 12; heavy.duplicate = 0.10; heavy.reorder = 0.20; heavy.reorder_delay_ns = 4'000'000; heavy.corrupt = 0.03; heavy.jitter_ns = 1'000'000;
  FaultConfig slowb = clean; slowb.base_delay_ns = 15'000'000;
  FaultConfig dead; dead.loss = 0.60; dead.burst_start = 0.004; dead.burst_len = 150;

  unsigned k = 0;
  CHECK(run({"clean, both lines", clean, clean}, base + 2 * k++, 1, verbose) == 0);
  CHECK(run({"mild faults on both lines", mild, mild}, base + 2 * k++, 2, verbose) == 0);
  CHECK(run({"heavy loss and bursts, B delayed", heavy, slowb}, base + 2 * k++, 3, verbose) == 0);
  CHECK(run({"A heavy, B light", heavy, mild}, base + 2 * k++, 4, verbose) == 0);
  CHECK(run({"both lines mostly dead -> server", dead, dead, 0.2}, base + 2 * k++, 5, verbose) == 0);
  CHECK(run({"late join (40% in), clean", clean, clean, 0.0, 15000, 0.4}, base + 2 * k++, 6, verbose) == 0);
  CHECK(run({"late join (60% in), mild + flaky server", mild, mild, 0.3, 15000, 0.6}, base + 2 * k++, 7, verbose) == 0);
  CHECK(run({"no re-request server, lossy", heavy, heavy, 0, 6000, 0.0, true}, base + 2 * k++, 8, verbose) == 0);
  std::puts("ok");
  return 0;
}

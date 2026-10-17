// The feed adapter: receives MoldUDP64 on two redundant lines, recovers gaps from a re-request server, builds
// order books, and (optionally) serves them to clients over Aeron.
//
//   itch_feed_handler --a 239.1.1.1:30001 --b 239.1.1.2:30002 --iface 10.0.0.5 --rerequest 10.0.0.9:40500 [--serve]
//
// Threads: the SocketManager's network thread (sockets, sequencing, recovery) feeds an SPSC ring; this thread
// (the "book thread") drains it into the BookManager and, with --serve, runs the Aeron MarketDataServer.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include <spdlog/spdlog.h>

#include "itch/feed/feed_consumer.hpp"
#include "itch/feed/feed_handler.hpp"
#ifdef ITCH_WITH_AERON
#include "Aeron.h"
#include "itch/messaging/aeron_transport.hpp"
#include "itch/messaging/market_data_server.hpp"
#endif

using namespace feed_handler;
using namespace feed_handler::feed;
using Clock = std::chrono::steady_clock;

namespace
{
std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop = true; }

bool parse_endpoint(const std::string &s, socket_address &out)
{
    const auto c = s.rfind(':');
    if (c == std::string::npos) return false;
    out.ip = s.substr(0, c);
    out.port = static_cast<uint16_t>(std::strtoul(s.c_str() + c + 1, nullptr, 10));
    return !out.ip.empty() && out.port != 0;
}

void usage()
{
    std::fputs(
        "usage: itch_feed_handler --a IP:PORT --b IP:PORT [options]\n"
        "  --a / --b IP:PORT       line A / line B: multicast group + port, or a unicast address to bind\n"
        "  --iface IP              local interface address for joining multicast\n"
        "  --rerequest IP:PORT     re-request server (needed to recover lost packets and to catch up)\n"
        "  --no-join-from-start    start at the first sequence seen instead of replaying from 1 (books will be incomplete)\n"
        "  --gap-wait-ms N         grace for the other line before asking the server (default 5)\n"
        "  --request-timeout-ms N  retry a request after this long (default 200)\n"
        "  --attempts N            then give up on a hole and go Stale (default 5)\n"
        "  --window N              re-requests in flight at once (default 8)\n"
        "  --auto-rebuild-ms N     after Stale, rebuild from sequence 1 by itself after this long (default off)\n"
        "  --cpu N                 pin the network thread to core N (busy-polls that core)\n"
        "  --ring-mb N             ring between the threads (default 128)\n"
        "  --rcvbuf-mb N           socket receive buffer (default 8)\n"
        "  --expect-orders N       pre-size the order store (default 4194304)\n"
        "  --seconds N             stop after N seconds (default: at end of session)\n"
        "  --stats-every S         print status every S seconds (default 2)\n"
        "  --digest                print the final books digest\n"
        "  --serve                 serve books over Aeron (see itch_md_client)         [needs a build with Aeron]\n"
        "  --udp-port N            UDP control port for remote Aeron clients (default 40100)\n"
        "  --aeron-dir DIR         use a running media driver (default: start an embedded one)\n",
        stderr);
}
} // namespace

int main(int argc, char **argv)
{
    spdlog::set_level(spdlog::level::warn);
    FeedHandlerConfig hc;
    int cpu = -1;
    size_t ring_mb = 128, expect_orders = BookManager::kDefaultExpectedOrders;
    double seconds = 0, stats_every = 2;
    bool digest = false, serve = false;
    unsigned udp_port = 40100;
    std::string aeron_dir;
    hc.udp.rcvbuf_bytes = 8 << 20;
    hc.feed.request_timeout_ns = 200'000'000;

    for (int i = 1; i < argc; ++i)
    {
        const std::string a = argv[i];
        auto next = [&]() -> const char * { return i + 1 < argc ? argv[++i] : (usage(), std::exit(2), nullptr); };
        if (a == "--a") { if (!parse_endpoint(next(), hc.line_a)) { usage(); return 2; } }
        else if (a == "--b") { if (!parse_endpoint(next(), hc.line_b)) { usage(); return 2; } }
        else if (a == "--iface") hc.udp.interface_ip = next();
        else if (a == "--rerequest") { if (!parse_endpoint(next(), hc.rerequest_server)) { usage(); return 2; } }
        else if (a == "--no-join-from-start") hc.feed.join_from_start = false;
        else if (a == "--gap-wait-ms") hc.feed.gap_wait_ns = static_cast<uint64_t>(std::strtod(next(), nullptr) * 1e6);
        else if (a == "--request-timeout-ms") hc.feed.request_timeout_ns = static_cast<uint64_t>(std::strtod(next(), nullptr) * 1e6);
        else if (a == "--attempts") hc.feed.max_request_attempts = static_cast<uint32_t>(std::strtoul(next(), nullptr, 10));
        else if (a == "--window") hc.feed.request_window = static_cast<uint32_t>(std::strtoul(next(), nullptr, 10));
        else if (a == "--auto-rebuild-ms") hc.feed.auto_rebuild_after_ns = static_cast<uint64_t>(std::strtod(next(), nullptr) * 1e6);
        else if (a == "--cpu") cpu = std::atoi(next());
        else if (a == "--ring-mb") ring_mb = std::strtoull(next(), nullptr, 10);
        else if (a == "--rcvbuf-mb") hc.udp.rcvbuf_bytes = static_cast<int>(std::strtoul(next(), nullptr, 10)) << 20;
        else if (a == "--expect-orders") expect_orders = std::strtoull(next(), nullptr, 10);
        else if (a == "--seconds") seconds = std::strtod(next(), nullptr);
        else if (a == "--stats-every") stats_every = std::strtod(next(), nullptr);
        else if (a == "--digest") digest = true;
        else if (a == "--serve") serve = true;
        else if (a == "--udp-port") udp_port = std::strtoul(next(), nullptr, 10);
        else if (a == "--aeron-dir") aeron_dir = next();
        else if (a == "-h" || a == "--help") { usage(); return 0; }
        else { usage(); return 2; }
    }
    if (hc.line_a.ip.empty() || hc.line_b.ip.empty()) { usage(); return 2; }
#ifndef ITCH_WITH_AERON
    if (serve) { std::fprintf(stderr, "--serve needs a build with Aeron (-DITCH_WITH_AERON=ON)\n"); return 2; }
#endif

    SocketManager sockets;
    SocketManager::NetworkThreadConfig nc;
    nc.cpu = cpu;
    if (sockets.Init(nc) != error_code::ok || sockets.Start() != error_code::ok) { std::fprintf(stderr, "could not start the network thread\n"); return 1; }

    SpscRing ring(ring_mb << 20);
    BookManager books(expect_orders);
    FeedConsumer consumer(ring, books);
    FeedHandler handler(sockets, hc, ring);

#ifdef ITCH_WITH_AERON
    std::unique_ptr<messaging::EmbeddedMediaDriver> driver;
    std::unique_ptr<messaging::AeronTransport> transport;
    std::unique_ptr<messaging::MarketDataServer> server;
    if (serve)
    {
        std::string err;
        if (aeron_dir.empty())
        {
            aeron_dir = aeron::Context::defaultAeronPath();
            messaging::EmbeddedMediaDriver::Options o;
            o.dir = aeron_dir;
            driver = messaging::EmbeddedMediaDriver::start(o, err);
            if (!driver) { std::fprintf(stderr, "%s\n", err.c_str()); return 1; }
        }
        transport = messaging::AeronTransport::connect(aeron_dir, err);
        if (!transport) { std::fprintf(stderr, "%s\n", err.c_str()); return 1; }
        messaging::ServerConfig sc;
        sc.control_udp_channel = messaging::make_udp_channel("0.0.0.0", static_cast<uint16_t>(udp_port));
        server = std::make_unique<messaging::MarketDataServer>(*transport, books, sc);
        if (!server->start()) { std::fprintf(stderr, "could not open Aeron control channels\n"); return 1; }
        FeedConsumer::Hooks hooks;
        hooks.on_live = [&] { server->refresh_snapshots(); };     // subscribers missed updates while catching up / rebuilding
        consumer.set_hooks(hooks);
        std::fprintf(stderr, "serving books over Aeron (dir %s, IPC + UDP port %u)\n", aeron_dir.c_str(), udp_port);
    }
#endif

    std::string err;
    if (!handler.Start(err)) { std::fprintf(stderr, "feed handler: %s\n", err.c_str()); return 1; }
    std::fprintf(stderr, "receiving: A=%s:%u B=%s:%u re-request=%s\n", hc.line_a.ip.c_str(), hc.line_a.port, hc.line_b.ip.c_str(), hc.line_b.port,
                 hc.rerequest_server.ip.empty() ? "none" : (hc.rerequest_server.ip + ":" + std::to_string(hc.rerequest_server.port)).c_str());

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    const auto start = Clock::now();
    auto last_stats = start;
    uint64_t spins = 0;
    auto report = [&] {
        const FeedStats s = handler.stats();
        std::fprintf(stderr,
                     "[%s] seq %llu/%llu applied %llu | A %llu pkts up=%d, B %llu pkts up=%d | won A/B %llu/%llu | gaps %llu req %llu retry %llu recovered %llu | dups %llu bad %llu | bufdrop %llu sendfail %llu | books %zu orders %zu\n",
                     to_string(s.state), (unsigned long long)s.next_sequence, (unsigned long long)s.highest_seen, (unsigned long long)consumer.messages(),
                     (unsigned long long)s.line[0].packets, s.line[0].up, (unsigned long long)s.line[1].packets, s.line[1].up,
                     (unsigned long long)s.line[0].messages_won, (unsigned long long)s.line[1].messages_won, (unsigned long long)s.gaps_detected,
                     (unsigned long long)s.requests_sent, (unsigned long long)s.request_retries, (unsigned long long)s.recovered_messages,
                     (unsigned long long)(s.line[0].duplicate_packets + s.line[1].duplicate_packets), (unsigned long long)(s.line[0].malformed + s.line[1].malformed),
                     (unsigned long long)s.buffer_drops, (unsigned long long)s.request_send_failures, books.book_count(), books.live_orders());
    };
    int exit_code = 0;
    while (!g_stop)
    {
        const int n = consumer.poll(4096);
#ifdef ITCH_WITH_AERON
        if (server && (++spins & 63) == 0) server->poll_control();
#endif
        if (n == 0) std::this_thread::sleep_for(std::chrono::microseconds(50));
        if (consumer.mode() == FeedConsumer::Mode::Ended) { std::fprintf(stderr, "end of session\n"); break; }
        if (consumer.mode() == FeedConsumer::Mode::Stale && hc.feed.auto_rebuild_after_ns == 0) { std::fprintf(stderr, "STALE: a gap could not be recovered; the books are no longer reliable\n"); exit_code = 3; break; }
        const auto now = Clock::now();
        if (stats_every > 0 && std::chrono::duration<double>(now - last_stats).count() >= stats_every) { last_stats = now; report(); }
        if (seconds > 0 && std::chrono::duration<double>(now - start).count() >= seconds) break;
    }
    for (int i = 0; i < 20; ++i) { consumer.poll(1 << 20); std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
    handler.Stop();
    consumer.poll(1 << 20);
    report();
    const FeedStats s = handler.stats();
    std::fprintf(stderr, "ring: data-full %llu, control-full %llu | decode failures %llu | store: %.1f MB, heap fallbacks %llu | validate_all: %s\n",
                 (unsigned long long)handler.ring_data_full(), (unsigned long long)handler.ring_control_full(), (unsigned long long)consumer.decode_failures(),
                 books.store_memory_bytes() / 1e6, (unsigned long long)books.store_stats().total(), books.validate_all() ? "ok" : "FAIL");
    (void)s;
    if (digest) std::printf("books digest: %016llx (%zu books, %zu live orders)\n", (unsigned long long)books.state_digest(), books.book_count(), books.live_orders());
#ifdef ITCH_WITH_AERON
    if (server) server->stop();
#endif
    sockets.Stop();
    return exit_code;
}

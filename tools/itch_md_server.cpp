// Market data server: replays a raw ITCH 5.0 file through the order books and serves
// client subscriptions (L2 depth up to 10, time & sales) over Aeron.
//
//   gzip -dc day.gz | itch_md_server --preload-until 10:00:00 --rate 200000
//
// Without --aeron-dir an embedded media driver is started in Aeron's default directory, so
// clients on this machine can connect with no extra flags. For real deployments run the
// standalone aeronmd on pinned cores and pass its directory.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>

#include "Aeron.h"
#include "itch/messaging/aeron_transport.hpp"
#include "itch/messaging/market_data_server.hpp"
#include "itch/protocol/file_reader.hpp"

using namespace feed_handler;
using namespace feed_handler::messaging;
using Clock = std::chrono::steady_clock;

namespace
{
std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop = true; }

uint64_t BE48(const char *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 6; ++i) v = (v << 8) | static_cast<unsigned char>(p[i]);
    return v;
}

bool parse_time(const char *s, uint64_t &ns)
{
    int h = 0, m = 0, sec = 0;
    if (std::sscanf(s, "%d:%d:%d", &h, &m, &sec) < 2) return false;
    ns = (uint64_t(h) * 3600 + uint64_t(m) * 60 + uint64_t(sec)) * 1000000000ull;
    return true;
}

void usage()
{
    std::fputs(
        "usage: itch_md_server [options] [file]      (reads stdin if no file)\n"
        "  --aeron-dir DIR         use a running media driver (default: start an embedded one)\n"
        "  --udp-port N            UDP control port for remote clients (default 40100)\n"
        "  --no-udp                IPC only\n"
        "  --mcast-interface IP    local interface address for multicast replies\n"
        "  --allow CIDR            only send UDP data to this network (repeatable; default: any)\n"
        "  --preload-until HH:MM:SS  process messages up to this time at full speed first\n"
        "  --stop-at HH:MM:SS      stop replaying at this time (default: end of input)\n"
        "  --rate N                messages per second after the preload (default 100000; 0 = unthrottled)\n"
        "  --linger SECONDS        keep serving after the input ends (default 0; useful for snapshots)\n"
        "  --expect-orders N       pre-size the order pool (default 4194304)\n",
        stderr);
}
} // namespace

int main(int argc, char **argv)
{
    spdlog::set_level(spdlog::level::warn);
    ServerConfig cfg;
    std::string aeron_dir, path;
    uint64_t preload_until = 0, stop_at = UINT64_MAX, rate = 100000, linger_s = 0;
    unsigned udp_port = 40100;
    bool udp = true;
    size_t expect_orders = BookManager::kDefaultExpectedOrders;

    for (int i = 1; i < argc; ++i)
    {
        const std::string a = argv[i];
        auto next = [&]() -> const char * { return i + 1 < argc ? argv[++i] : (usage(), std::exit(2), nullptr); };
        if (a == "--aeron-dir") aeron_dir = next();
        else if (a == "--udp-port") udp_port = std::strtoul(next(), nullptr, 10);
        else if (a == "--no-udp") udp = false;
        else if (a == "--mcast-interface") cfg.multicast_interface = next();
        else if (a == "--allow")
        {
            const auto c = parse_cidr(next());
            if (!c) { std::fprintf(stderr, "bad --allow CIDR\n"); return 2; }
            cfg.allowed_reply_cidrs.push_back(*c);
        }
        else if (a == "--preload-until") { if (!parse_time(next(), preload_until)) { usage(); return 2; } }
        else if (a == "--stop-at") { if (!parse_time(next(), stop_at)) { usage(); return 2; } }
        else if (a == "--rate") rate = std::strtoull(next(), nullptr, 10);
        else if (a == "--linger") linger_s = std::strtoull(next(), nullptr, 10);
        else if (a == "--expect-orders") expect_orders = std::strtoull(next(), nullptr, 10);
        else if (a == "-h" || a == "--help") { usage(); return 0; }
        else if (a[0] != '-') path = a;
        else { usage(); return 2; }
    }
    cfg.control_udp_channel = udp ? make_udp_channel("0.0.0.0", static_cast<uint16_t>(udp_port)) : "";
    if (cfg.allowed_reply_cidrs.empty())
        std::fprintf(stderr, "note: no --allow given, so UDP data may be sent to any address a client asks for (development only)\n");

    std::string err;
    std::unique_ptr<EmbeddedMediaDriver> driver;
    if (aeron_dir.empty())
    {
        aeron_dir = aeron::Context::defaultAeronPath();
        EmbeddedMediaDriver::Options o;
        o.dir = aeron_dir;
        driver = EmbeddedMediaDriver::start(o, err);
        if (!driver) { std::fprintf(stderr, "%s\n", err.c_str()); return 1; }
    }
    auto transport = AeronTransport::connect(aeron_dir, err);
    if (!transport) { std::fprintf(stderr, "%s\n", err.c_str()); return 1; }

    BookManager books(expect_orders);
    MarketDataServer server(*transport, books, cfg);
    if (!server.start()) { std::fprintf(stderr, "could not open control channels\n"); return 1; }
    std::fprintf(stderr, "serving: aeron dir %s | IPC control %s stream %d%s%s\n", aeron_dir.c_str(),
                 cfg.control_ipc_channel.c_str(), cfg.control_stream_id, udp ? " | UDP control port " : "",
                 udp ? std::to_string(udp_port).c_str() : "");

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    std::FILE *f = path.empty() ? stdin : std::fopen(path.c_str(), "rb");
    if (!f) { std::perror("open"); return 1; }
    std::setvbuf(f, nullptr, _IOFBF, 1 << 20);

    itch::FileReader reader(f);
    NasdaqItchDecoder decoder;
    uint64_t n = 0, paced = 0, last_ts = 0;
    bool pacing = preload_until == 0;
    Clock::time_point pace_start = Clock::now(), last_report = Clock::now();
    const char *data; size_t len;

    auto report = [&] {
        const auto &s = server.stats();
        std::fprintf(stderr, "[%02d:%02d:%02d] msgs=%llu clients=%zu l2=%llu snap=%llu trades=%llu drops(bp=%llu nc=%llu) books=%llu\n",
                     int(last_ts / 3600000000000ull), int(last_ts / 60000000000ull % 60), int(last_ts / 1000000000ull % 60),
                     (unsigned long long)n, server.client_count(), (unsigned long long)s.l2_sent, (unsigned long long)s.snapshots_sent,
                     (unsigned long long)s.trades_sent, (unsigned long long)s.dropped_back_pressure,
                     (unsigned long long)s.dropped_not_connected, (unsigned long long)books.stats().book_messages);
    };

    while (!g_stop)
    {
        const auto st = reader.Next(data, len);
        if (st != itch::FileReader::Status::Ok) break;
        if (len < 11) continue;
        last_ts = BE48(data + 5);
        if (last_ts >= stop_at) break;
        if (!pacing && last_ts >= preload_until)
        {
            pacing = true;
            pace_start = Clock::now();
            std::fprintf(stderr, "preload done at %02d:%02d:%02d, %llu messages; now pacing at %llu msg/s\n",
                         int(last_ts / 3600000000000ull), int(last_ts / 60000000000ull % 60), int(last_ts / 1000000000ull % 60),
                         (unsigned long long)n, (unsigned long long)rate);
            report();
        }
        size_t used = 0;
        decoder.DecodeMessage(data, len, used, books);
        ++n;
        if ((n & 63) == 0) server.poll_control();
        if (pacing && rate && ++paced % 256 == 0)
        {
            const auto due = pace_start + std::chrono::nanoseconds(paced * 1000000000ull / rate);
            while (Clock::now() < due && !g_stop) { server.poll_control(); std::this_thread::sleep_for(std::chrono::microseconds(50)); }
        }
        if (Clock::now() - last_report > std::chrono::seconds(2)) { last_report = Clock::now(); report(); }
    }

    std::fprintf(stderr, "input ended after %llu messages\n", (unsigned long long)n);
    const auto until = Clock::now() + std::chrono::seconds(linger_s);
    while (!g_stop && Clock::now() < until) { server.poll_control(); std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
    report();
    server.stop();
    return 0;
}

// Market data client: subscribes to instruments and prints what the server sends.
//
//   itch_md_client --symbols AAPL,MSFT --l2 5 --tns                    (same machine, IPC)
//   itch_md_client --server 10.0.0.1 --reply-host 10.0.0.7 --symbols AAPL --l2 10
//   itch_md_client --server 10.0.0.1 --reply-host 10.0.0.7 --mcast 239.1.1.1 --symbols AAPL --tns
#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "Aeron.h"
#include "itch/messaging/aeron_transport.hpp"
#include "itch/messaging/market_data_client.hpp"

using namespace feed_handler;
using namespace feed_handler::messaging;
using Clock = std::chrono::steady_clock;

namespace
{
std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop = true; }

std::string px(int32_t p) { char b[32]; std::snprintf(b, sizeof b, "%d.%04d", p / 10000, std::abs(p) % 10000); return b; }
std::string tod(uint64_t ns) { char b[40]; std::snprintf(b, sizeof b, "%02d:%02d:%02d.%09d", int(ns / 3600000000000ull), int(ns / 60000000000ull % 60), int(ns / 1000000000ull % 60), int(ns % 1000000000ull)); return b; }

struct Printer : IMarketDataHandler
{
    std::map<uint16_t, std::string> names;
    std::vector<double> latency_us;        // server send -> client read, wall clocks (valid on one machine)
    uint64_t every = 1, l2_seen = 0, trades_seen = 0, gaps = 0;
    bool quiet = false;
    bool server_lost = false;

    const char *name(uint16_t loc) const { auto it = names.find(loc); return it == names.end() ? "?" : it->second.c_str(); }
    static double now_us_after(uint64_t send_ns)
    {
        const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        return (static_cast<int64_t>(now) - static_cast<int64_t>(send_ns)) / 1e3;
    }
    void on_ack(const AckView &a) override
    {
        std::printf("ack request %u: %s\n", a.request_id, to_string(a.status));
        for (uint8_t i = 0; i < a.result_count; ++i)
            std::printf("   result %u: locate %u %s\n", i, a.results[i].locate, to_string(static_cast<Status>(a.results[i].status)));
    }
    void on_l2(const L2View &v) override
    {
        ++l2_seen;
        latency_us.push_back(now_us_after(v.send_ts_ns));
        if (quiet || (!v.snapshot && l2_seen % every)) return;
        std::printf("\n%s  L2 %s seq %llu  exch %s\n", name(v.locate), v.snapshot ? "SNAPSHOT" : "update", (unsigned long long)v.sequence, tod(v.exchange_ts_ns).c_str());
        for (int i = v.ask_count - 1; i >= 0; --i) std::printf("   ASK %12s x %-8llu (%u)\n", px(v.asks[i].price).c_str(), (unsigned long long)v.asks[i].shares, v.asks[i].orders);
        std::printf("   ---------\n");
        for (int i = 0; i < v.bid_count; ++i) std::printf("   BID %12s x %-8llu (%u)\n", px(v.bids[i].price).c_str(), (unsigned long long)v.bids[i].shares, v.bids[i].orders);
    }
    void on_trade(const TradeView &t) override
    {
        ++trades_seen;
        latency_us.push_back(now_us_after(t.send_ts_ns));
        if (quiet) return;
        std::printf("%s  TRADE %c%c%c %s x %llu  match %llu  seq %llu  exch %s\n", name(t.locate), t.kind, t.side ? ' ' : ' ',
                    t.side ? t.side : '-', px(t.price).c_str(), (unsigned long long)t.shares, (unsigned long long)t.match_number,
                    (unsigned long long)t.sequence, tod(t.exchange_ts_ns).c_str());
    }
    void on_gap(uint16_t loc, StreamKind k, uint64_t expected, uint64_t got) override
    {
        ++gaps;
        std::printf("!! GAP %s %s: expected seq %llu, got %llu\n", name(loc), k == StreamKind::L2 ? "L2" : "trades",
                    (unsigned long long)expected, (unsigned long long)got);
    }
    void on_server_timeout() override { server_lost = true; std::printf("!! server stopped responding\n"); }
};

void usage()
{
    std::fputs(
        "usage: itch_md_client --symbols A,B,C [--l2 DEPTH] [--tns] [options]\n"
        "  --l2 DEPTH           order book depth 1..10\n"
        "  --tns                time & sales (prints)\n"
        "  --server ipc|IPV4    ipc = same machine (default), or the server's address\n"
        "  --control-port N     server UDP control port (default 40100)\n"
        "  --reply-host IPV4    this machine's address as the server sees it (required for UDP)\n"
        "  --reply-port N       UDP port to receive on (default 40200)\n"
        "  --mcast GROUP        receive on a multicast group instead of unicast\n"
        "  --reply-stream N     reply stream id (default random); clients using the SAME group, port and\n"
        "                       stream share one server publication and see each other's data (filtered locally)\n"
        "  --aeron-dir DIR      media driver directory (default: Aeron's default)\n"
        "  --seconds N          run time (default: until Ctrl-C)\n"
        "  --print-every N      print every Nth L2 update (default 1); snapshots always print\n"
        "  --quiet              count only, print the summary\n",
        stderr);
}
} // namespace

int main(int argc, char **argv)
{
    ClientConfig cc;
    std::string aeron_dir, symbols;
    uint8_t features = 0, depth = 0;
    uint64_t seconds = 0;
    Printer pr;

    for (int i = 1; i < argc; ++i)
    {
        const std::string a = argv[i];
        auto next = [&]() -> const char * { return i + 1 < argc ? argv[++i] : (usage(), std::exit(2), nullptr); };
        if (a == "--symbols") symbols = next();
        else if (a == "--l2") { features |= kFeatureL2Book; depth = static_cast<uint8_t>(std::strtoul(next(), nullptr, 10)); }
        else if (a == "--tns") features |= kFeatureTimeAndSales;
        else if (a == "--server") cc.server_host = next();
        else if (a == "--control-port") cc.control_port = static_cast<uint16_t>(std::strtoul(next(), nullptr, 10));
        else if (a == "--reply-host") cc.reply_host = next();
        else if (a == "--reply-port") cc.reply_port = static_cast<uint16_t>(std::strtoul(next(), nullptr, 10));
        else if (a == "--mcast") cc.multicast_group = next();
        else if (a == "--reply-stream") cc.reply_stream_id = static_cast<int32_t>(std::strtol(next(), nullptr, 10));
        else if (a == "--aeron-dir") aeron_dir = next();
        else if (a == "--seconds") seconds = std::strtoull(next(), nullptr, 10);
        else if (a == "--print-every") pr.every = std::max<uint64_t>(1, std::strtoull(next(), nullptr, 10));
        else if (a == "--quiet") pr.quiet = true;
        else { usage(); return 2; }
    }
    std::vector<std::string> syms;
    for (size_t pos = 0; pos <= symbols.size();)
    {
        size_t c = symbols.find(',', pos);
        if (c == std::string::npos) c = symbols.size();
        if (c > pos) syms.push_back(symbols.substr(pos, c - pos));
        pos = c + 1;
    }
    if (syms.empty() || !features) { usage(); return 2; }

    std::string err;
    auto transport = AeronTransport::connect(aeron_dir.empty() ? aeron::Context::defaultAeronPath() : aeron_dir, err);
    if (!transport) { std::fprintf(stderr, "%s\n", err.c_str()); return 1; }
    MarketDataClient client(*transport, cc);
    if (!client.connect()) { std::fprintf(stderr, "connect failed: %s\n", client.last_error().c_str()); return 1; }
    std::fprintf(stderr, "connected (client id %llx, reply stream %d)\n", (unsigned long long)client.client_id(), client.reply_stream_id());

    if (!client.subscribe(syms, features, depth)) { std::fprintf(stderr, "subscribe request could not be sent\n"); return 1; }
    // Remember which locate is which symbol once the ack arrives.
    struct Namer : IMarketDataHandler {
        Printer &p; const std::vector<std::string> &syms;
        Namer(Printer &p_, const std::vector<std::string> &s) : p(p_), syms(s) {}
        void on_ack(const AckView &a) override { for (uint8_t i = 0; i < a.result_count && i < syms.size(); ++i) if (a.results[i].locate) p.names[a.results[i].locate] = syms[i]; p.on_ack(a); }
        void on_l2(const L2View &v) override { p.on_l2(v); }
        void on_trade(const TradeView &t) override { p.on_trade(t); }
        void on_gap(uint16_t l, StreamKind k, uint64_t e, uint64_t g) override { p.on_gap(l, k, e, g); }
        void on_server_timeout() override { p.on_server_timeout(); }
    } handler(pr, syms);

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    const auto start = Clock::now();
    while (!g_stop && (!seconds || Clock::now() - start < std::chrono::seconds(seconds)))
    {
        if (client.poll(handler) == 0) std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    client.disconnect();
    for (int i = 0; i < 20; ++i) { client.poll(handler); std::this_thread::sleep_for(std::chrono::milliseconds(5)); }

    const auto &s = client.stats();
    std::printf("\nsummary: L2 messages %llu (snapshots %llu), trades %llu, gaps %llu, malformed %llu, server heartbeats %llu, ignored (not subscribed) %llu\n",
                (unsigned long long)s.l2, (unsigned long long)s.snapshots, (unsigned long long)s.trades, (unsigned long long)s.gaps,
                (unsigned long long)s.malformed, (unsigned long long)s.heartbeats, (unsigned long long)s.filtered);
    if (!pr.latency_us.empty())
    {
        auto v = pr.latency_us;
        std::sort(v.begin(), v.end());
        std::printf("server-send to client-read: p50 %.0f us, p99 %.0f us, max %.0f us (wall clocks; meaningful only on one machine, and includes this tool's polling sleep)\n",
                    v[v.size() / 2], v[v.size() * 99 / 100], v.back());
    }
    return pr.server_lost ? 1 : 0;
}

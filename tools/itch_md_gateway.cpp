// TCP gateway: lets clients without Aeron (Python, Windows, ...) use the market data server over one
// outbound TCP connection. Run it next to the server (same machine, IPC) or on any machine that can
// reach the server over Aeron UDP. See docs/messaging.md.
//
//   itch_md_gateway --listen 0.0.0.0 --port 40300 --allow 10.0.0.0/24
//
// SECURITY: no authentication. It listens on 127.0.0.1 unless --listen says otherwise; use --allow, a
// private network, a VPN or an SSH tunnel before exposing it.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "Aeron.h"
#include "itch/messaging/aeron_transport.hpp"
#include "itch/messaging/tcp_gateway.hpp"

using namespace feed_handler::messaging;
using Clock = std::chrono::steady_clock;

namespace
{
std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop = true; }

void usage()
{
    std::fputs(
        "usage: itch_md_gateway [options]\n"
        "  --listen IPV4           address to listen on (default 127.0.0.1; 0.0.0.0 = all interfaces)\n"
        "  --port N                TCP port (default 40300)\n"
        "  --allow CIDR            only accept peers from this network (repeatable; default: any)\n"
        "  --max-connections N     (default 64)\n"
        "  --server ipc|IPV4       how to reach the market data server: ipc = same machine (default),\n"
        "                          or its address over Aeron UDP\n"
        "  --control-port N        server's UDP control port (default 40100)\n"
        "  --reply-host IPV4       this machine's address as the server sees it (required for UDP)\n"
        "  --reply-port N          first UDP reply port; each connection uses the next one (default 40400)\n"
        "  --aeron-dir DIR         media driver directory (default: Aeron's default)\n",
        stderr);
}
} // namespace

int main(int argc, char **argv)
{
    GatewayConfig cfg;
    cfg.upstream.connect_timeout = std::chrono::milliseconds(500);   // a missing server must not stall other connections
    cfg.upstream.reply_port = 40400;
    std::string aeron_dir;

    for (int i = 1; i < argc; ++i)
    {
        const std::string a = argv[i];
        auto next = [&]() -> const char * { return i + 1 < argc ? argv[++i] : (usage(), std::exit(2), nullptr); };
        if (a == "--listen") cfg.listen_host = next();
        else if (a == "--port") cfg.listen_port = static_cast<uint16_t>(std::strtoul(next(), nullptr, 10));
        else if (a == "--allow")
        {
            const auto c = parse_cidr(next());
            if (!c) { std::fprintf(stderr, "bad --allow CIDR\n"); return 2; }
            cfg.allowed_peers.push_back(*c);
        }
        else if (a == "--max-connections") cfg.max_connections = std::strtoul(next(), nullptr, 10);
        else if (a == "--server") cfg.upstream.server_host = next();
        else if (a == "--control-port") cfg.upstream.control_port = static_cast<uint16_t>(std::strtoul(next(), nullptr, 10));
        else if (a == "--reply-host") cfg.upstream.reply_host = next();
        else if (a == "--reply-port") cfg.upstream.reply_port = static_cast<uint16_t>(std::strtoul(next(), nullptr, 10));
        else if (a == "--aeron-dir") aeron_dir = next();
        else if (a == "-h" || a == "--help") { usage(); return 0; }
        else { usage(); return 2; }
    }
    if (cfg.listen_host != "127.0.0.1" && cfg.allowed_peers.empty())
        std::fprintf(stderr, "warning: listening on %s with no --allow: anyone who can reach the port gets market data\n",
                     cfg.listen_host.c_str());

    std::string err;
    auto transport = AeronTransport::connect(aeron_dir.empty() ? aeron::Context::defaultAeronPath() : aeron_dir, err);
    if (!transport) { std::fprintf(stderr, "%s\n(is itch_md_server or aeronmd running?)\n", err.c_str()); return 1; }

    TcpGateway gw(*transport, cfg);
    if (!gw.start(err)) { std::fprintf(stderr, "%s\n", err.c_str()); return 1; }
    std::fprintf(stderr, "gateway listening on %s:%u, upstream %s\n", cfg.listen_host.c_str(), gw.port(),
                 cfg.upstream.server_host == "ipc" ? "Aeron IPC" : cfg.upstream.server_host.c_str());

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    auto last = Clock::now();
    while (!g_stop)
    {
        if (gw.poll() == 0) std::this_thread::sleep_for(std::chrono::microseconds(50));
        if (Clock::now() - last > std::chrono::seconds(5))
        {
            last = Clock::now();
            const auto &s = gw.stats();
            std::fprintf(stderr, "connections=%zu accepted=%llu out=%llu msgs (%llu bytes) closed: slow=%llu malformed=%llu idle=%llu upstream=%llu\n",
                         gw.connection_count(), (unsigned long long)s.accepted, (unsigned long long)s.messages_out,
                         (unsigned long long)s.bytes_out, (unsigned long long)s.closed_slow, (unsigned long long)s.closed_malformed,
                         (unsigned long long)s.closed_idle, (unsigned long long)s.closed_upstream);
        }
    }
    gw.stop();
    return 0;
}

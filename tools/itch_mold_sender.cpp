// Replays a raw ITCH 5.0 file (2-byte length-prefixed messages) as a MoldUDP64 feed on two redundant lines,
// with optional fault injection, and serves re-requests. For exercising itch_feed_handler without an exchange.
//
//   gzip -dc day.gz | itch_mold_sender --a 239.1.1.1:30001 --b 239.1.1.2:30002 --iface 10.0.0.5 \
//        --rate 200000 --loss 0.05 --dup 0.02 --reorder 0.1:3 --rerequest-port 40500 --max-messages 5000000
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include "itch/feed/mold_sender.hpp"
#include "itch/protocol/file_reader.hpp"

using namespace feed_handler;
using namespace feed_handler::feed;
using Clock = std::chrono::steady_clock;

namespace
{
std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop = true; }
uint64_t now_ns() { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count()); }

bool parse_endpoint(const std::string &s, std::string &ip, uint16_t &port)
{
    const auto c = s.rfind(':');
    if (c == std::string::npos) return false;
    ip = s.substr(0, c);
    port = static_cast<uint16_t>(std::strtoul(s.c_str() + c + 1, nullptr, 10));
    return !ip.empty() && port != 0;
}

void usage()
{
    std::fputs(
        "usage: itch_mold_sender --a IP:PORT --b IP:PORT [options] [file]     (stdin if no file)\n"
        "  --a / --b IP:PORT       destination of line A / line B (unicast address or multicast group)\n"
        "  --iface IP              outgoing interface for multicast\n"
        "  --session NAME          MoldUDP64 session (10 chars, default SESSION001)\n"
        "  --rate N                messages per second (default 200000; 0 = as fast as possible)\n"
        "  --max-messages N        stop after N messages (default: whole input)\n"
        "  --start-delay-ms N      wait before the first packet (time to start the receiver)\n"
        "  --max-packet N          bytes per packet (default 1400)\n"
        "  --rerequest-port N      serve re-requests on this UDP port (0 = do not serve)\n"
        "  --reply-drop P          ignore this fraction of re-requests\n"
        "  --linger-s N            keep running this long after the end of session (default 2)\n"
        "faults (probabilities 0..1; --loss/--dup/--corrupt apply to both lines, -a/-b suffix to one):\n"
        "  --loss P  --loss-a P  --loss-b P      drop packets\n"
        "  --burst P:LEN                         start a burst of LEN consecutive drops with probability P\n"
        "  --dup P  --corrupt P                  duplicate / mangle packets\n"
        "  --reorder P:MS                        hold a packet back MS milliseconds with probability P\n"
        "  --delay-b MS                          line B lags line A by MS\n"
        "  --seed N\n",
        stderr);
}
} // namespace

int main(int argc, char **argv)
{
    std::string a_ep, b_ep, iface, session = "SESSION001", path;
    uint64_t rate = 200000, max_messages = UINT64_MAX, start_delay_ms = 0, linger_s = 2, seed = 1;
    size_t max_packet = mold::kDefaultMaxPayload;
    unsigned rerequest_port = 0;
    double reply_drop = 0;
    FaultConfig fa, fb;

    for (int i = 1; i < argc; ++i)
    {
        const std::string a = argv[i];
        auto next = [&]() -> const char * { return i + 1 < argc ? argv[++i] : (usage(), std::exit(2), nullptr); };
        auto both = [&](double FaultConfig::*m, double v) { fa.*m = v; fb.*m = v; };
        if (a == "--a") a_ep = next();
        else if (a == "--b") b_ep = next();
        else if (a == "--iface") iface = next();
        else if (a == "--session") session = next();
        else if (a == "--rate") rate = std::strtoull(next(), nullptr, 10);
        else if (a == "--max-messages") max_messages = std::strtoull(next(), nullptr, 10);
        else if (a == "--start-delay-ms") start_delay_ms = std::strtoull(next(), nullptr, 10);
        else if (a == "--max-packet") max_packet = std::strtoull(next(), nullptr, 10);
        else if (a == "--rerequest-port") rerequest_port = std::strtoul(next(), nullptr, 10);
        else if (a == "--reply-drop") reply_drop = std::strtod(next(), nullptr);
        else if (a == "--linger-s") linger_s = std::strtoull(next(), nullptr, 10);
        else if (a == "--seed") seed = std::strtoull(next(), nullptr, 10);
        else if (a == "--loss") both(&FaultConfig::loss, std::strtod(next(), nullptr));
        else if (a == "--loss-a") fa.loss = std::strtod(next(), nullptr);
        else if (a == "--loss-b") fb.loss = std::strtod(next(), nullptr);
        else if (a == "--dup") both(&FaultConfig::duplicate, std::strtod(next(), nullptr));
        else if (a == "--corrupt") both(&FaultConfig::corrupt, std::strtod(next(), nullptr));
        else if (a == "--burst")
        {
            double p; unsigned len;
            if (std::sscanf(next(), "%lf:%u", &p, &len) != 2) { usage(); return 2; }
            fa.burst_start = fb.burst_start = p; fa.burst_len = fb.burst_len = len;
        }
        else if (a == "--reorder")
        {
            double p, ms;
            if (std::sscanf(next(), "%lf:%lf", &p, &ms) != 2) { usage(); return 2; }
            fa.reorder = fb.reorder = p; fa.reorder_delay_ns = fb.reorder_delay_ns = static_cast<uint64_t>(ms * 1e6);
        }
        else if (a == "--delay-b") fb.base_delay_ns = static_cast<uint64_t>(std::strtod(next(), nullptr) * 1e6);
        else if (a == "-h" || a == "--help") { usage(); return 0; }
        else if (a[0] != '-') path = a;
        else { usage(); return 2; }
    }
    std::string a_ip, b_ip;
    uint16_t a_port = 0, b_port = 0;
    if (!parse_endpoint(a_ep, a_ip, a_port) || !parse_endpoint(b_ep, b_ip, b_port)) { usage(); return 2; }

    UdpSender tx_a, tx_b;
    std::string err;
    if (!tx_a.open(a_ip, a_port, iface, 4, err) || !tx_b.open(b_ip, b_port, iface, 4, err)) { std::fprintf(stderr, "%s\n", err.c_str()); return 1; }

    MessageLog log;
    std::atomic<uint64_t> sent_next{1};
    MoldFeedSender::Config mc;
    mc.session = mold::Session(session);
    mc.max_packet_bytes = max_packet;
    mc.fault_a = fa; mc.fault_b = fb; mc.seed = seed;
    MoldFeedSender sender(mc, tx_a, tx_b, log, sent_next);

    std::unique_ptr<UdpRerequestServer> server;
    if (rerequest_port)
    {
        UdpRerequestServer::Options so;
        so.drop_probability = reply_drop; so.seed = seed; so.max_packet_bytes = max_packet;
        server = std::make_unique<UdpRerequestServer>(mc.session, log, sent_next, so);
        if (!server->start("0.0.0.0", static_cast<uint16_t>(rerequest_port), err)) { std::fprintf(stderr, "re-request server: %s\n", err.c_str()); return 1; }
        std::fprintf(stderr, "re-request server on UDP %u\n", server->port());
    }

    std::FILE *f = path.empty() ? stdin : std::fopen(path.c_str(), "rb");
    if (!f) { std::perror("open"); return 1; }
    std::setvbuf(f, nullptr, _IOFBF, 1 << 20);
    itch::FileReader reader(f);

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::fprintf(stderr, "sending session %s to A=%s B=%s at %llu msg/s\n", session.c_str(), a_ep.c_str(), b_ep.c_str(), (unsigned long long)rate);
    if (start_delay_ms) std::this_thread::sleep_for(std::chrono::milliseconds(start_delay_ms));

    const uint64_t t0 = now_ns();
    uint64_t n = 0, last_report = t0;
    const char *data; size_t len;
    while (!g_stop && n < max_messages && reader.Next(data, len) == itch::FileReader::Status::Ok)
    {
        if (len == 0) continue;
        sender.publish(reinterpret_cast<const uint8_t *>(data), len, now_ns());
        ++n;
        if ((n & 63) == 0) sender.service(now_ns());
        if (rate && n % 256 == 0)
        {
            const uint64_t due = t0 + n * 1000000000ull / rate;
            for (uint64_t t = now_ns(); t < due && !g_stop; t = now_ns()) { sender.service(t); std::this_thread::sleep_for(std::chrono::microseconds(50)); }
        }
        if (now_ns() - last_report > 2'000'000'000ull)
        {
            last_report = now_ns();
            std::fprintf(stderr, "sent %llu messages, %llu packets (A lost %llu, B lost %llu)\n", (unsigned long long)n, (unsigned long long)sender.packets_sent(),
                         (unsigned long long)sender.dropped(0), (unsigned long long)sender.dropped(1));
        }
    }
    sender.end_session(now_ns());
    std::fprintf(stderr, "input done after %llu messages; announcing end of session\n", (unsigned long long)n);
    const uint64_t end0 = now_ns();
    while (!g_stop && (!sender.finished(now_ns()) || now_ns() - end0 < linger_s * 1000000000ull))
    {
        sender.service(now_ns());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::fprintf(stderr, "done: %llu messages, %llu packets | line A: %llu lost %llu dup %llu corrupt | line B: %llu lost %llu dup %llu corrupt | re-requests received %llu, served %llu, ignored %llu\n",
                 (unsigned long long)n, (unsigned long long)sender.packets_sent(), (unsigned long long)sender.dropped(0), (unsigned long long)sender.duplicated(0),
                 (unsigned long long)sender.corrupted(0), (unsigned long long)sender.dropped(1), (unsigned long long)sender.duplicated(1), (unsigned long long)sender.corrupted(1),
                 server ? (unsigned long long)server->received() : 0ull, server ? (unsigned long long)server->served() : 0ull, server ? (unsigned long long)server->ignored() : 0ull);
    return 0;
}

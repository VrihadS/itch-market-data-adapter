#include "itch/messaging/tcp_gateway.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <unordered_map>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "itch/messaging/protocol.hpp"

namespace feed_handler::messaging
{

namespace
{
uint64_t steady_now_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
uint64_t realtime_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_DONTWAIT | MSG_NOSIGNAL;
#else
constexpr int kSendFlags = MSG_DONTWAIT;   // macOS: SIGPIPE is disabled per socket (SO_NOSIGPIPE)
#endif

void set_nonblocking(int fd) { fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK); }
} // namespace

// One TCP peer and the upstream client that serves it.
struct TcpGateway::Connection final : IMarketDataHandler
{
    TcpGateway *gw = nullptr;
    int fd = -1;
    int slot = -1;
    std::string peer;

    std::vector<uint8_t> in, out;
    size_t out_off = 0;

    std::unique_ptr<MarketDataClient> upstream;
    uint64_t peer_client_id = 0;
    std::unordered_map<uint32_t, uint32_t> req_map;    // upstream request id -> the peer's request id
    uint32_t disconnect_req = 0;                       // upstream id of a pending disconnect, 0 if none

    uint64_t last_rx_ns = 0, last_hb_ns = 0;
    bool drop = false;                 // close now
    bool close_after_flush = false;    // close once queued output is written

    void queue(const uint8_t *data, size_t len)
    {
        if (out.size() - out_off + len > gw->cfg_.max_pending_output_bytes)
        {
            ++gw->stats_.closed_slow;     // the peer is not keeping up: never buffer without bound
            drop = true;
            return;
        }
        out.insert(out.end(), data, data + len);
        ++gw->stats_.messages_out;
    }

    void send_ack(uint32_t peer_request_id, Status status, uint8_t result_count = 0, const SymbolResult *results = nullptr)
    {
        SubscribeAck a{};
        a.client_id = peer_client_id;
        a.request_id = peer_request_id;
        a.status = static_cast<uint8_t>(status);
        a.result_count = result_count;
        for (uint8_t i = 0; i < result_count; ++i) a.results[i] = results[i];
        uint8_t buf[kMaxMessageSize];
        queue(buf, encode(a, buf));
    }

    // ── upstream -> peer ──────────────────────────────────────────
    void on_ack(const AckView &a) override
    {
        uint32_t peer_id = 0;
        const auto it = req_map.find(a.request_id);
        if (it != req_map.end()) { peer_id = it->second; req_map.erase(it); }
        send_ack(peer_id, a.status, a.result_count, a.results);
        if (disconnect_req && a.request_id == disconnect_req) close_after_flush = true;
    }
    void on_l2(const L2View &v) override
    {
        L2Book h{};
        h.locate = v.locate;
        h.bid_count = v.bid_count;
        h.ask_count = v.ask_count;
        h.snapshot = v.snapshot ? 1 : 0;
        h.sequence = v.sequence;
        h.exchange_ts_ns = v.exchange_ts_ns;
        h.send_ts_ns = v.send_ts_ns;
        uint8_t buf[kMaxL2MessageSize];
        queue(buf, encode_l2(h, v.bids, v.asks, buf));
    }
    void on_trade(const TradeView &t) override
    {
        Trade m{};
        m.locate = t.locate;
        m.kind = t.kind;
        m.side = t.side;
        m.cross_type = t.cross_type;
        m.sequence = t.sequence;
        m.exchange_ts_ns = t.exchange_ts_ns;
        m.send_ts_ns = t.send_ts_ns;
        m.price = t.price;
        m.shares = t.shares;
        m.match_number = t.match_number;
        uint8_t buf[kMaxMessageSize];
        queue(buf, encode(m, buf));
    }
    void on_server_timeout() override
    {
        ++gw->stats_.closed_upstream;     // the market data server went quiet: let the peer reconnect
        drop = true;
    }

    bool ensure_upstream()
    {
        if (upstream) return true;
        ClientConfig cc = gw->cfg_.upstream;
        cc.client_id = 0;
        cc.reply_stream_id = 0;
        cc.multicast_group.clear();
        if (cc.server_host != "ipc") cc.reply_port = static_cast<uint16_t>(cc.reply_port + slot);
        auto c = std::make_unique<MarketDataClient>(gw->transport_, cc);
        if (!c->connect()) return false;
        upstream = std::move(c);
        return true;
    }
};

TcpGateway::TcpGateway(ITransport &t, GatewayConfig cfg) : transport_(t), cfg_(std::move(cfg))
{
    if (!cfg_.now_ns) cfg_.now_ns = steady_now_ns;
    slot_used_.assign(cfg_.max_connections, false);
}

TcpGateway::~TcpGateway() { stop(); }

bool TcpGateway::start(std::string &error)
{
    if (listen_fd_ >= 0) return true;
    const auto ip = parse_ipv4(cfg_.listen_host);
    if (!ip) { error = "listen_host must be an IPv4 address"; return false; }
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { error = std::string("socket: ") + std::strerror(errno); return false; }
    const int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(*ip);
    a.sin_port = htons(cfg_.listen_port);
    if (::bind(fd, reinterpret_cast<sockaddr *>(&a), sizeof a) < 0 || ::listen(fd, 32) < 0)
    {
        error = "bind/listen on " + cfg_.listen_host + ":" + std::to_string(cfg_.listen_port) + ": " + std::strerror(errno);
        ::close(fd);
        return false;
    }
    socklen_t len = sizeof a;
    ::getsockname(fd, reinterpret_cast<sockaddr *>(&a), &len);
    bound_port_ = ntohs(a.sin_port);
    set_nonblocking(fd);
    listen_fd_ = fd;
    return true;
}

void TcpGateway::stop()
{
    for (auto &c : conns_) close_connection(*c);
    conns_.clear();
    if (listen_fd_ >= 0) { ::close(listen_fd_); listen_fd_ = -1; }
}

void TcpGateway::close_connection(Connection &c)
{
    if (c.upstream && c.upstream->connected() && c.disconnect_req == 0)
        c.upstream->disconnect();             // best effort: lets the server drop the subscriptions now
    c.upstream.reset();
    if (c.fd >= 0) { ::close(c.fd); c.fd = -1; }
    if (c.slot >= 0) { slot_used_[c.slot] = false; c.slot = -1; }
}

void TcpGateway::accept_new()
{
    for (int i = 0; i < 16; ++i)
    {
        sockaddr_in pa{};
        socklen_t pl = sizeof pa;
        const int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr *>(&pa), &pl);
        if (fd < 0) return;
        const uint32_t pip = ntohl(pa.sin_addr.s_addr);
        if (!cfg_.allowed_peers.empty() &&
            std::none_of(cfg_.allowed_peers.begin(), cfg_.allowed_peers.end(), [&](const Cidr &c) { return c.contains(pip); }))
        {
            ++stats_.rejected_peer;
            ::close(fd);
            continue;
        }
        const auto free_slot = std::find(slot_used_.begin(), slot_used_.end(), false);
        if (free_slot == slot_used_.end()) { ++stats_.rejected_full; ::close(fd); continue; }
        set_nonblocking(fd);
        const int on = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof on);
#ifdef SO_NOSIGPIPE
        ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof on);
#endif
        auto c = std::make_unique<Connection>();
        c->gw = this;
        c->fd = fd;
        c->slot = static_cast<int>(free_slot - slot_used_.begin());
        slot_used_[c->slot] = true;
        c->peer = format_ipv4(pip) + ":" + std::to_string(ntohs(pa.sin_port));
        c->last_rx_ns = c->last_hb_ns = now();
        conns_.push_back(std::move(c));
        ++stats_.accepted;
    }
}

void TcpGateway::handle_message(Connection &c, const uint8_t *data, size_t len)
{
    ++stats_.messages_in;
    MsgType t;
    if (!peek_header(data, len, t)) { ++stats_.closed_malformed; c.drop = true; return; }
    switch (t)
    {
    case MsgType::SubscribeRequest:
    {
        SubscribeRequest r;
        if (!decode(data, len, r)) { ++stats_.closed_malformed; c.drop = true; return; }
        c.peer_client_id = r.client_id;
        if (!c.ensure_upstream())
        {
            ++stats_.upstream_failed;
            c.send_ack(r.request_id, Status::Unavailable);
            c.close_after_flush = true;
            return;
        }
        std::vector<std::string> symbols;
        for (uint8_t i = 0; i < r.symbol_count; ++i) symbols.push_back(*get_symbol(r.symbols[i]));
        const uint32_t id = c.upstream->subscribe(symbols, r.features, r.depth);
        if (!id) { c.send_ack(r.request_id, Status::BadRequest); return; }
        if (c.req_map.size() > 1024) c.req_map.clear();
        c.req_map[id] = r.request_id;
        break;
    }
    case MsgType::UnsubscribeRequest:
    {
        UnsubscribeRequest r;
        if (!decode(data, len, r)) { ++stats_.closed_malformed; c.drop = true; return; }
        c.peer_client_id = r.client_id;
        if (!c.upstream)
        {
            if (r.symbol_count == 0) { c.drop = true; return; }
            SymbolResult res[kMaxSymbolsPerRequest] = {};
            for (uint8_t i = 0; i < r.symbol_count; ++i) res[i].status = static_cast<uint8_t>(Status::NotSubscribed);
            c.send_ack(r.request_id, Status::Ok, r.symbol_count, res);
            return;
        }
        if (r.symbol_count == 0)    // leave: ask the server to forget us, close once it has answered
        {
            const uint32_t id = c.upstream->disconnect();
            if (!id) { c.drop = true; return; }
            c.disconnect_req = id;
            c.req_map[id] = r.request_id;
            return;
        }
        std::vector<std::string> symbols;
        for (uint8_t i = 0; i < r.symbol_count; ++i) symbols.push_back(*get_symbol(r.symbols[i]));
        const uint32_t id = c.upstream->unsubscribe(symbols, r.features);
        if (!id) { c.send_ack(r.request_id, Status::BadRequest); return; }
        c.req_map[id] = r.request_id;
        break;
    }
    case MsgType::Heartbeat:
        break;                          // receipt already refreshed the idle timer
    default:                            // a peer must not send server-to-client messages
        ++stats_.closed_malformed;
        c.drop = true;
    }
}

bool TcpGateway::service(Connection &c, uint64_t t)
{
    // 1. read what the peer sent and handle complete messages
    if (!c.close_after_flush)
    {
        uint8_t buf[65536];
        for (int i = 0; i < 4; ++i)
        {
            const ssize_t n = ::recv(c.fd, buf, sizeof buf, MSG_DONTWAIT);
            if (n > 0)
            {
                c.last_rx_ns = t;
                c.in.insert(c.in.end(), buf, buf + n);
                if (c.in.size() > 4 * kMaxMessageSize + sizeof buf) { ++stats_.closed_malformed; return false; }
            }
            else if (n == 0) return false;                                  // peer closed
            else if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            else if (errno != EINTR) return false;
            if (static_cast<size_t>(n) < sizeof buf) break;
        }
        size_t off = 0;
        while (c.in.size() - off >= sizeof(Header) && !c.drop)
        {
            Header h;
            std::memcpy(&h, c.in.data() + off, sizeof h);
            if (h.length < sizeof(Header) || h.length > kMaxMessageSize) { ++stats_.closed_malformed; return false; }
            if (c.in.size() - off < h.length) break;                        // wait for the rest
            handle_message(c, c.in.data() + off, h.length);
            off += h.length;
        }
        c.in.erase(c.in.begin(), c.in.begin() + static_cast<std::ptrdiff_t>(off));
    }
    if (c.drop) return false;

    // 2. pump the upstream client (acks, snapshots, updates, prints) into the output queue
    if (c.upstream) c.upstream->poll(c, 64);
    if (c.drop) return false;

    // 3. keep the peer's liveness check fed
    if (t - c.last_hb_ns >= cfg_.heartbeat_interval_ns)
    {
        c.last_hb_ns = t;
        Heartbeat hb{};
        hb.client_id = c.peer_client_id;
        hb.send_ts_ns = realtime_ns();
        hb.from_server = 1;
        uint8_t buf[kMaxMessageSize];
        c.queue(buf, encode(hb, buf));
    }

    // 4. write as much as the socket takes without blocking
    while (c.out_off < c.out.size())
    {
        const ssize_t n = ::send(c.fd, c.out.data() + c.out_off, c.out.size() - c.out_off, kSendFlags);
        if (n > 0) { c.out_off += static_cast<size_t>(n); stats_.bytes_out += static_cast<uint64_t>(n); }
        else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        else if (n < 0 && errno == EINTR) continue;
        else return false;
    }
    if (c.out_off == c.out.size()) { c.out.clear(); c.out_off = 0; }
    else if (c.out_off > (1u << 20)) { c.out.erase(c.out.begin(), c.out.begin() + static_cast<std::ptrdiff_t>(c.out_off)); c.out_off = 0; }
    if (c.drop) return false;

    // 5. housekeeping
    if (c.close_after_flush && c.out.empty()) return false;
    if (t - c.last_rx_ns > cfg_.idle_timeout_ns) { ++stats_.closed_idle; return false; }
    return true;
}

int TcpGateway::poll()
{
    if (listen_fd_ < 0) return 0;
    const size_t before = conns_.size();
    accept_new();
    int work = static_cast<int>(conns_.size() - before);
    const uint64_t t = now();
    for (size_t i = 0; i < conns_.size();)
    {
        const uint64_t out0 = stats_.bytes_out, in0 = stats_.messages_in;
        if (service(*conns_[i], t))
        {
            work += (stats_.bytes_out != out0) + (stats_.messages_in != in0);
            ++i;
            continue;
        }
        close_connection(*conns_[i]);
        conns_.erase(conns_.begin() + static_cast<std::ptrdiff_t>(i));
        ++work;
    }
    return work;
}

} // namespace feed_handler::messaging

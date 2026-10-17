#pragma once
// TCP front door for clients that cannot (or should not) run Aeron: Python, Windows, anything
// with sockets. It speaks the SAME binary protocol as the Aeron path, framed by the message
// header's length field, over one outbound TCP connection per client:
//
//   Python/Windows client --TCP--> TcpGateway --MarketDataClient (Aeron IPC / UDP)--> MarketDataServer
//
// Each TCP connection gets its own upstream MarketDataClient, so subscriptions, snapshots,
// sequence numbers and L2/trade semantics are exactly those of a native client. The gateway
// IGNORES the reply channel in a client's request: where data goes is decided by the gateway,
// never by the TCP peer.
//
// Trade-offs vs. native Aeron: one extra hop, and TCP head-of-line blocking. Good for
// prototyping, analytics and strategies that tolerate milliseconds; not for the latency-critical
// path. POSIX only (Linux / macOS): run it beside the server. The CLIENT can be anything.
//
// THREADING: single-threaded; call poll() from one thread. Never blocks on a slow peer: each
// connection has a bounded output queue and is closed when it overflows.
//
// SECURITY: no authentication. It listens on 127.0.0.1 unless told otherwise; use `allowed_peers`,
// a private network, a VPN or an SSH tunnel before exposing it.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "itch/messaging/channel.hpp"
#include "itch/messaging/market_data_client.hpp"
#include "itch/messaging/transport.hpp"

namespace feed_handler::messaging {

struct GatewayConfig {
    std::string listen_host = "127.0.0.1";   // 0.0.0.0 to accept from other machines (see SECURITY)
    uint16_t    listen_port = 40300;         // 0 = pick a free port (see TcpGateway::port())
    std::vector<Cidr> allowed_peers;         // empty = any peer that can reach the port

    size_t   max_connections          = 64;
    size_t   max_pending_output_bytes = 8u << 20;       // per connection; beyond this the peer is too slow and is closed
    uint64_t idle_timeout_ns          = 30'000'000'000ull;   // nothing received from the peer for this long -> closed
    uint64_t heartbeat_interval_ns    = 1'000'000'000ull;

    // How each connection's upstream client reaches the market data server. server_host is
    // "ipc" when the gateway runs on the server's machine. For UDP, each connection uses
    // upstream.reply_port + its slot number as its own reply port. A short connect_timeout
    // keeps a missing server from stalling the other connections.
    ClientConfig upstream;

    std::function<uint64_t()> now_ns;        // monotonic clock; default steady_clock (tests inject)
};

struct GatewayStats {
    uint64_t accepted = 0, rejected_peer = 0, rejected_full = 0;
    uint64_t closed_slow = 0, closed_malformed = 0, closed_idle = 0, closed_upstream = 0;
    uint64_t messages_out = 0, bytes_out = 0, messages_in = 0, upstream_failed = 0;
};

class TcpGateway {
public:
    TcpGateway(ITransport &upstream_transport, GatewayConfig cfg);
    ~TcpGateway();
    TcpGateway(const TcpGateway &) = delete;
    TcpGateway &operator=(const TcpGateway &) = delete;

    bool start(std::string &error);   // binds and listens
    void stop();                      // closes every connection (and its upstream subscriptions)
    int  poll();                      // accept, read, write, pump upstream clients; returns work done

    uint16_t port() const { return bound_port_; }
    size_t   connection_count() const { return conns_.size(); }
    const GatewayStats &stats() const { return stats_; }

private:
    struct Connection;
    void accept_new();
    bool service(Connection &c, uint64_t now);          // false -> remove
    void close_connection(Connection &c);
    void handle_message(Connection &c, const uint8_t *data, size_t len);
    uint64_t now() const { return cfg_.now_ns(); }

    ITransport  &transport_;
    GatewayConfig cfg_;
    GatewayStats  stats_;
    int           listen_fd_ = -1;
    uint16_t      bound_port_ = 0;
    std::vector<std::unique_ptr<Connection>> conns_;
    std::vector<bool>                        slot_used_;
};

} // namespace feed_handler::messaging

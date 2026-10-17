#pragma once
// Client side of the market data protocol.
//
//   MarketDataClient c(transport, cfg);
//   c.connect();
//   c.subscribe({"AAPL","MSFT"}, kFeatureL2Book | kFeatureTimeAndSales, 10);
//   while (running) c.poll(handler);
//
// Same machine: set server_host = "ipc" (Aeron IPC, shared memory through the local media
// driver). Another machine: set server_host to the server's IPv4 address and reply_host to
// this machine's address as the server sees it (UDP unicast), or set multicast_group to
// receive over a UDP multicast group. All clients of one server need distinct client_ids and,
// for IPC / unicast, distinct reply streams or ports; the defaults are random.
//
// THREADING: a client is single-threaded; call connect/subscribe/poll from one thread.

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "itch/messaging/protocol.hpp"
#include "itch/messaging/transport.hpp"

namespace feed_handler::messaging {

struct ClientConfig {
    uint64_t    client_id = 0;              // 0 = pick a random id
    std::string server_host = "ipc";        // "ipc" or the server's IPv4 address
    uint16_t    control_port = 40100;
    int32_t     control_stream_id = 1001;

    std::string reply_host;                 // this machine's IPv4 as reachable by the server (UDP)
    uint16_t    reply_port = 40200;         // UDP port this client listens on
    int32_t     reply_stream_id = 0;        // 0 = pick a random stream id
    std::string multicast_group;            // non-empty: receive on this group (port = reply_port)

    std::chrono::milliseconds connect_timeout{5000};
    uint64_t heartbeat_interval_ns = 1'000'000'000ull;
    uint64_t server_timeout_ns     = 5'000'000'000ull;   // nothing heard from the server for this long
    std::function<uint64_t()> now_ns;                    // monotonic clock; default steady_clock
};

struct AckView {
    uint32_t request_id;
    Status   status;                       // request-level result
    uint8_t  result_count;                 // per-symbol results, valid when status == Ok
    SymbolResult results[kMaxSymbolsPerRequest];
};

struct L2View {
    uint16_t locate;
    bool     snapshot;
    uint64_t sequence, exchange_ts_ns, send_ts_ns;
    uint8_t  bid_count, ask_count;
    BookLevel bids[kMaxL2Depth];           // best first
    BookLevel asks[kMaxL2Depth];
};

struct TradeView {
    uint16_t locate;
    char     kind, side, cross_type;
    uint64_t sequence, exchange_ts_ns, send_ts_ns;
    int32_t  price;
    uint64_t shares, match_number;
};

enum class StreamKind : uint8_t { L2 = 0, Trades = 1 };

class IMarketDataHandler {
public:
    virtual ~IMarketDataHandler() = default;
    virtual void on_ack(const AckView &) {}
    virtual void on_l2(const L2View &) {}
    virtual void on_trade(const TradeView &) {}
    // A sequence number was skipped: `expected` was missed, `got` arrived. For L2 the next
    // update (or a re-subscribe, which sends a fresh snapshot) repairs the book.
    virtual void on_gap(uint16_t /*locate*/, StreamKind /*kind*/, uint64_t /*expected*/, uint64_t /*got*/) {}
    virtual void on_server_timeout() {}    // once, when nothing has arrived for server_timeout_ns
};

struct ClientStats {
    uint64_t acks = 0, l2 = 0, snapshots = 0, trades = 0, heartbeats = 0;
    uint64_t gaps = 0, malformed = 0;
    uint64_t requests_dropped = 0;         // control offers that could not be sent
    uint64_t filtered = 0;                 // data for instruments/features this client did not ask for (shared multicast streams)
};

class MarketDataClient {
public:
    MarketDataClient(ITransport &transport, ClientConfig cfg);

    // Opens the reply subscription and the control publication; waits for the server.
    bool connect();
    bool connected() const { return connected_; }
    const std::string &last_error() const { return error_; }

    // Returns the request id (matched by AckView::request_id), or 0 if it could not be sent.
    // `depth` is 1..kMaxL2Depth when features include kFeatureL2Book. At most
    // kMaxSymbolsPerRequest symbols per call. Re-subscribing replaces the earlier request for
    // those symbols and returns a fresh L2 snapshot.
    uint32_t subscribe(const std::vector<std::string> &symbols, uint8_t features, uint8_t depth = 0);
    uint32_t unsubscribe(const std::vector<std::string> &symbols, uint8_t features);
    uint32_t disconnect();                 // drop every subscription and leave

    // Delivers incoming messages to `handler` and sends heartbeats. Returns messages handled.
    int poll(IMarketDataHandler &handler, int fragment_limit = 64);

    uint64_t client_id() const { return cfg_.client_id; }
    int32_t  reply_stream_id() const { return cfg_.reply_stream_id; }
    const ClientStats &stats() const { return stats_; }

private:
    struct PollCtx { MarketDataClient *self; IMarketDataHandler *handler; };
    static void on_fragment(void *ctx, const uint8_t *data, size_t len);
    void        dispatch(IMarketDataHandler &h, const uint8_t *data, size_t len);
    void        check_gap(IMarketDataHandler &h, uint16_t locate, StreamKind kind, uint64_t seq, bool resets);
    bool        send(const uint8_t *data, size_t len);
    uint64_t    now() const { return cfg_.now_ns(); }

    ITransport  &transport_;
    ClientConfig cfg_;
    ClientStats  stats_;
    std::string  error_;
    bool         connected_ = false;
    bool         timeout_reported_ = false;
    uint32_t     next_request_ = 1;
    uint64_t     last_heard_ns_ = 0, last_heartbeat_ns_ = 0;
    std::string  request_channel_;        // what the server is told (strict form)

    std::unique_ptr<IPublication>  control_;
    std::unique_ptr<ISubscription> reply_;
    std::unordered_map<uint32_t, uint64_t> expected_;   // (locate << 1 | kind) -> next expected sequence

    // On a multicast stream shared with other clients the server publishes the union of what
    // the group asked for, so each client keeps its own subscriptions and drops the rest.
    struct PendingRequest { std::vector<std::string> symbols; uint8_t features; bool unsubscribe; bool disconnect; };
    std::unordered_map<uint32_t, PendingRequest> pending_;
    std::unordered_map<uint16_t, uint8_t>        subscribed_;   // locate -> features confirmed by the server
    void apply_ack(const SubscribeAck &a);
};

} // namespace feed_handler::messaging

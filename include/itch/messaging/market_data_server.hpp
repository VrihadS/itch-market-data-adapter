#pragma once
// Publishes order book depth and time & sales to subscribed clients.
//
//   feed thread:  decoder -> BookManager -> MarketDataServer (BookListener) -> IPublication
//                                              ^ poll_control() between messages
//
// Clients send a SubscribeRequest (symbols, features, depth, reply channel). The server
// resolves symbols to locates, records the subscription, creates a publication to the
// client's reply channel (IPC, UDP unicast, or a UDP multicast group), acknowledges, and
// then streams only what that client asked for. L2 subscribers first receive a snapshot,
// so they can join at any time.
//
// THREADING: everything here, including the BookListener callbacks and poll_control(),
// must run on the thread that feeds the BookManager. The server never blocks that thread:
// publication set-up is asynchronous and a full channel drops (and counts) rather than waits.

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "itch/book/book_manager.hpp"
#include "itch/messaging/channel.hpp"
#include "itch/messaging/protocol.hpp"
#include "itch/messaging/transport.hpp"

namespace feed_handler::messaging {

struct ServerConfig {
    // Where clients send requests. IPC serves clients on this machine; UDP serves the rest.
    std::string control_ipc_channel = "aeron:ipc";
    std::string control_udp_channel = "aeron:udp?endpoint=0.0.0.0:40100";   // empty = no UDP control
    int32_t     control_stream_id   = 1001;

    // Multicast replies are sent from this local interface address (appended as |interface=).
    std::string multicast_interface;

    // Destinations the server will send UDP data to. EMPTY MEANS ANY: fine for development,
    // but in production list your client networks, or anyone who can reach the control port
    // can aim the feed at an arbitrary address.
    std::vector<Cidr> allowed_reply_cidrs;

    size_t max_clients                = 256;
    size_t max_subscriptions_per_client = 256;

    uint64_t client_timeout_ns     = 10'000'000'000ull;   // no heartbeat/request for this long -> dropped
    uint64_t connect_timeout_ns    = 5'000'000'000ull;    // reply channel never connected -> dropped
    uint64_t heartbeat_interval_ns = 1'000'000'000ull;    // server -> client liveness message
    uint64_t tick_interval_ns      = 50'000'000ull;       // housekeeping cadence inside poll_control()

    std::function<uint64_t()> now_ns;                     // monotonic clock; default steady_clock (tests inject)
};

struct ServerStats {
    uint64_t requests = 0, accepted = 0, rejected = 0;     // subscribe requests
    uint64_t dropped_unreplyable = 0;                      // bad / disallowed reply channel: no way to answer
    uint64_t malformed = 0;                                // control messages that failed validation
    uint64_t clients_expired = 0, clients_dropped = 0;     // timeout vs. unreachable reply channel
    uint64_t l2_sent = 0, snapshots_sent = 0, trades_sent = 0;
    uint64_t dropped_back_pressure = 0, dropped_not_connected = 0;
};

class MarketDataServer final : public BookListener {
public:
    MarketDataServer(ITransport &transport, BookManager &books, ServerConfig cfg = {});
    ~MarketDataServer() override;

    bool start();         // creates the control subscriptions, registers as the book listener
    void stop();          // clears all interest and unregisters
    int  poll_control();  // returns control messages handled; call often (e.g. every N feed messages)
    // Sends every L2 subscriber a fresh snapshot (e.g. after the books were rebuilt or caught up, when
    // updates were withheld). Clients treat a snapshot as a re-base of their book.
    void refresh_snapshots();

    // BookListener (hot path)
    void on_book_update(uint16_t locate, const OrderBook &book, uint64_t exchange_ts_ns) override;
    void on_trade(const TradeEvent &ev) override;

    const ServerStats &stats() const { return stats_; }
    size_t client_count() const { return clients_.size(); }
    size_t sink_count() const { return sinks_.size(); }

private:
    struct Sink {
        std::string key, channel;
        int32_t     stream_id = 0;
        ChannelKind kind = ChannelKind::Invalid;
        PendingId   pending = 0;
        std::unique_ptr<IPublication> pub;
        bool        active = false;           // publication exists and a subscriber is attached
        bool        ever_connected = false;
        uint64_t    created_ns = 0, last_connected_ns = 0, last_heartbeat_ns = 0;
        uint64_t    owner = 0;                // sole client for IPC/unicast sinks; 0 = shared (multicast)
        int         refs = 0;
        std::vector<std::vector<uint8_t>> queued;   // control messages waiting for the sink to connect
        std::vector<uint16_t>             snapshot_wanted;
    };
    struct SubInfo { uint8_t features; uint8_t depth; };
    struct Client {
        uint64_t id = 0;
        Sink    *sink = nullptr;
        uint64_t last_seen_ns = 0;
        std::unordered_map<uint16_t, SubInfo> subs;
    };
    // What one reply sink wants for one locate (union over the clients sharing the sink).
    struct Route {
        Sink    *sink;
        uint8_t  features;
        uint8_t  depth;
        uint64_t l2_seq;
        uint64_t trade_seq;
    };

    static void on_control_fragment(void *ctx, const uint8_t *data, size_t len);
    void        handle_control(const uint8_t *data, size_t len);
    void        handle_subscribe(const SubscribeRequest &r);
    void        handle_unsubscribe(const UnsubscribeRequest &r);
    void        handle_heartbeat(const Heartbeat &h);

    Sink *get_or_create_sink(const ParsedChannel &pc, std::string_view channel, int32_t stream_id,
                             uint64_t client_id, Status &status);
    void  release_sink(Sink *s);
    void  remove_client(uint64_t id);
    void  rebuild_routes(uint16_t locate);

    void send_or_queue(Sink &s, const uint8_t *data, size_t len);
    void on_sink_active(Sink &s);
    void flush_snapshots(Sink &s);
    void send_snapshot(Route &r, uint16_t locate);
    bool offer(Sink &s, const uint8_t *data, size_t len);   // counts drops; true if accepted

    void tick(uint64_t now);
    uint64_t now() const { return cfg_.now_ns(); }

    ITransport  &transport_;
    BookManager &books_;
    ServerConfig cfg_;
    ServerStats  stats_;
    bool         started_ = false;
    uint64_t     last_tick_ns_ = 0;

    std::vector<std::unique_ptr<ISubscription>> control_subs_;
    std::map<std::string, std::unique_ptr<Sink>> sinks_;
    std::unordered_map<uint64_t, Client>         clients_;
    std::vector<std::vector<Route>>              routes_;   // indexed by locate
};

} // namespace feed_handler::messaging

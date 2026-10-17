#pragma once
// The network half of the feed adapter.
//
//   line A (multicast) --\
//   line B (multicast) ---+--> FeedSession --> RingSink --> SpscRing --> (book thread) FeedConsumer
//   re-request server  --/        ^   |
//   (unicast replies)             |   +-- requests --> re-request server
//                              1 ms tick
//
// Everything here runs on the SocketManager's network thread. The book thread reads the ring.

#include <atomic>
#include <memory>
#include <string>

#include "itch/feed/feed_records.hpp"
#include "itch/feed/feed_session.hpp"
#include "itch/net/socket_manager.h"

namespace feed_handler::feed {

struct FeedHandlerConfig {
    socket_address line_a;                 // multicast group + port, or a unicast address to bind
    socket_address line_b;
    socket_address rerequest_server;       // ip empty = no recovery (a gap then ends in Stale)
    uint16_t       rerequest_local_port = 0;   // 0 = ephemeral
    FeedConfig     feed;
    UdpOptions     udp;                    // interface_ip, receive buffer, ...
    uint64_t       tick_interval_ns = 1'000'000;
};

class FeedHandler {
public:
    FeedHandler(SocketManager &sockets, FeedHandlerConfig cfg, SpscRing &ring);
    ~FeedHandler();
    FeedHandler(const FeedHandler &) = delete;
    FeedHandler &operator=(const FeedHandler &) = delete;

    // Opens the sockets and starts the timer. The SocketManager must be Init()ed and Start()ed.
    bool Start(std::string &error);
    void Stop();   // after this returns no packet is processed and the ring gets nothing more

    FeedStats stats() const { return session_->stats(); }
    State state() const { return session_->state(); }
    uint64_t ring_data_full() const { return sink_.data_full(); }
    uint64_t ring_control_full() const { return sink_.control_full(); }
    uint64_t rerequest_bytes_received() const;
    uint64_t foreign_rerequest_packets() const { return foreign_.load(); }

    // Network-thread access for tests / the tool (e.g. rebuild after Stale). Safe from any thread: runs there.
    void RequestRebuild();

private:
    struct LineCallback;
    class RerequestChannel;
    void ScheduleTick();

    SocketManager &sockets_;
    FeedHandlerConfig cfg_;
    RingSink sink_;
    std::unique_ptr<RerequestChannel> rerequest_;
    std::unique_ptr<FeedSession> session_;

    std::unique_ptr<LineCallback> cb_a_, cb_b_, cb_r_;
    std::shared_ptr<UDPSocket> a_, b_;
    std::shared_ptr<UnicastUDPSocket> r_;
    std::unique_ptr<boost::asio::steady_timer> timer_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> foreign_{0};
    uint32_t server_ip_ = 0;
};

}  // namespace feed_handler::feed

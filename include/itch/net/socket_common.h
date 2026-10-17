#pragma once
#include <atomic>
#include <cstdint>
#include <string>

#include "itch/common/common.h"

namespace feed_handler
{

enum class TCPMode
{
    Standard,
    IOUring,
    // TCPDirect — future
};

struct socket_address
{
    std::string ip;
    uint16_t    port = 0;
};

// Per-datagram metadata handed to SocketCallback::OnDatagram.
struct RxInfo
{
    uint64_t rx_ns       = 0;   // steady_clock nanoseconds when the datagram was read from the socket
    uint32_t source_ip   = 0;   // sender, host byte order
    uint16_t source_port = 0;
};

class SocketCallback
{
public:
    virtual ~SocketCallback() {}

    // Override whichever suits: receivers call OnDatagram, whose default forwards to OnMessage.
    // Callbacks run on the network thread and must not block.
    virtual void OnMessage(const char * /*data*/, std::size_t /*len*/) {}
    virtual void OnDatagram(const char *data, std::size_t len, const RxInfo & /*info*/) { OnMessage(data, len); }
    virtual void OnConnect() {}
    virtual void OnDisconnect() {}   // the socket FAILED (not called for an intentional Stop)
};

// Tunables shared by the UDP receivers.
struct UdpOptions
{
    std::string interface_ip;                 // local interface for joining multicast / sending ("" = default route)
    int         rcvbuf_bytes = 8 << 20;       // requested SO_RCVBUF; the effective size is reported and warned about
    bool        reuse_port   = true;          // SO_REUSEPORT (lets lines A and B, or several processes, share a port)
    bool        multicast_loopback = true;    // deliver multicast sent from this host (tests, single-box setups)
    bool        bind_to_group = true;         // bind to the group address, so only that group's traffic on the port arrives
                                              // (a wildcard bind receives every group joined on that port by anything on the host)
    uint32_t    max_drain    = 64;            // datagrams read per reactor wakeup before yielding back
    uint32_t    max_consecutive_errors = 100; // then the socket is declared failed
};

// Counters other threads may read.
struct SocketStats
{
    std::atomic<uint64_t> packets{0}, bytes{0}, errors{0}, send_errors{0};
    std::atomic<uint64_t> wakeups{0};         // reactor wakeups; packets / wakeups = average batch
    std::atomic<uint64_t> max_batch{0};
    std::atomic<uint64_t> last_rx_ns{0};
};

}   // namespace feed_handler

#pragma once
// Strict handling of the reply channel a client asks the server to publish to.
//
// The server creates an Aeron publication for whatever channel the client names, so the
// string must never be passed through unchecked: Aeron channel URIs carry parameters
// (term length, control endpoints, ...) that a client must not be able to set. Only two
// shapes are accepted:
//
//     aeron:ipc
//     aeron:udp?endpoint=<IPv4>:<port>        (unicast, or multicast if the IPv4 is 224.0.0.0/4)
//
// Anything else, including extra parameters, hostnames and whitespace, is rejected.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace feed_handler::messaging {

enum class ChannelKind { Invalid, Ipc, UdpUnicast, UdpMulticast };

struct ParsedChannel {
    ChannelKind kind = ChannelKind::Invalid;
    uint32_t    ip   = 0;    // host byte order; 0 for IPC
    uint16_t    port = 0;
};

ParsedChannel parse_reply_channel(std::string_view channel);

std::optional<uint32_t> parse_ipv4(std::string_view text);   // "a.b.c.d" -> host-order value
std::string             format_ipv4(uint32_t ip);

// IPv4 prefix, e.g. 10.0.0.0/8, used to restrict where the server will send data.
struct Cidr {
    uint32_t network = 0;
    uint32_t mask    = 0;
    bool contains(uint32_t ip) const { return (ip & mask) == network; }
};
std::optional<Cidr> parse_cidr(std::string_view text);   // "10.1.2.0/24" or a bare address (/32)

// Channel strings the server actually uses.
std::string make_udp_channel(uint32_t ip, uint16_t port);
std::string make_udp_channel(std::string_view host, uint16_t port);

} // namespace feed_handler::messaging

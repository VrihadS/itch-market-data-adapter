#include "itch/messaging/channel.hpp"

#include <charconv>

namespace feed_handler::messaging
{

namespace
{
constexpr std::string_view kIpc = "aeron:ipc";
constexpr std::string_view kUdpPrefix = "aeron:udp?endpoint=";

bool parse_uint(std::string_view s, unsigned max, unsigned &out)
{
    if (s.empty() || s.size() > 5) return false;
    for (char c : s)
        if (c < '0' || c > '9') return false;
    if (s.size() > 1 && s[0] == '0') return false;    // no leading zeros ("010" is ambiguous in some parsers)
    unsigned v = 0;
    auto r = std::from_chars(s.data(), s.data() + s.size(), v);
    if (r.ec != std::errc() || r.ptr != s.data() + s.size() || v > max) return false;
    out = v;
    return true;
}
} // namespace

std::optional<uint32_t> parse_ipv4(std::string_view text)
{
    uint32_t ip = 0;
    for (int part = 0; part < 4; ++part)
    {
        const size_t dot = text.find('.');
        std::string_view piece = part < 3 ? text.substr(0, dot) : text;
        if (part < 3 && dot == std::string_view::npos) return std::nullopt;
        unsigned v;
        if (!parse_uint(piece, 255, v)) return std::nullopt;
        ip = (ip << 8) | v;
        if (part < 3) text.remove_prefix(dot + 1);
    }
    return ip;
}

std::string format_ipv4(uint32_t ip)
{
    return std::to_string(ip >> 24) + "." + std::to_string((ip >> 16) & 0xFF) + "." +
           std::to_string((ip >> 8) & 0xFF) + "." + std::to_string(ip & 0xFF);
}

std::optional<Cidr> parse_cidr(std::string_view text)
{
    unsigned prefix = 32;
    const size_t slash = text.find('/');
    if (slash != std::string_view::npos)
    {
        if (!parse_uint(text.substr(slash + 1), 32, prefix)) return std::nullopt;
        text = text.substr(0, slash);
    }
    const auto ip = parse_ipv4(text);
    if (!ip) return std::nullopt;
    Cidr c;
    c.mask    = prefix == 0 ? 0 : (0xFFFFFFFFu << (32 - prefix));
    c.network = *ip & c.mask;
    return c;
}

ParsedChannel parse_reply_channel(std::string_view channel)
{
    if (channel == kIpc) return {ChannelKind::Ipc, 0, 0};
    if (channel.substr(0, kUdpPrefix.size()) != kUdpPrefix) return {};
    channel.remove_prefix(kUdpPrefix.size());

    const size_t colon = channel.find(':');
    if (colon == std::string_view::npos) return {};
    const auto ip = parse_ipv4(channel.substr(0, colon));
    unsigned port;
    if (!ip || !parse_uint(channel.substr(colon + 1), 65535, port) || port == 0) return {};

    const uint32_t a = *ip >> 24;
    if (a == 0 || *ip == 0xFFFFFFFFu) return {};                 // 0.0.0.0/8 and limited broadcast
    const bool multicast = a >= 224 && a <= 239;
    if (a >= 240) return {};                                     // reserved
    return {multicast ? ChannelKind::UdpMulticast : ChannelKind::UdpUnicast, *ip, static_cast<uint16_t>(port)};
}

std::string make_udp_channel(uint32_t ip, uint16_t port)
{
    return std::string(kUdpPrefix) + format_ipv4(ip) + ":" + std::to_string(port);
}

std::string make_udp_channel(std::string_view host, uint16_t port)
{
    return std::string(kUdpPrefix) + std::string(host) + ":" + std::to_string(port);
}

} // namespace feed_handler::messaging

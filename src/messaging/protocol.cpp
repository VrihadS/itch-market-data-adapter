#include "itch/messaging/protocol.hpp"

namespace feed_handler::messaging {

const char *to_string(Status s)
{
    switch (s)
    {
    case Status::Ok:                   return "ok";
    case Status::UnknownSymbol:        return "unknown symbol";
    case Status::BadDepth:             return "bad depth";
    case Status::BadFeatures:          return "bad features";
    case Status::BadChannel:           return "bad reply channel";
    case Status::TooManyClients:       return "too many clients";
    case Status::TooManySubscriptions: return "too many subscriptions";
    case Status::BadRequest:           return "bad request";
    case Status::ChannelInUse:         return "reply channel in use";
    case Status::NotSubscribed:        return "not subscribed";
    case Status::Unavailable:          return "market data server unavailable";
    }
    return "unknown status";
}

namespace
{

template <class T>
bool check_fixed(const uint8_t *data, size_t len, MsgType want, T &out)
{
    MsgType t;
    if (!peek_header(data, len, t) || t != want || len != sizeof(T)) return false;
    std::memcpy(&out, data, sizeof(T));
    return true;
}

template <class T>
size_t put(const T &m, uint8_t *out)
{
    std::memcpy(out, &m, sizeof(T));
    return sizeof(T);
}

bool symbols_ok(const char (*symbols)[kSymbolLen], size_t count)
{
    for (size_t i = 0; i < count; ++i)
        if (!get_symbol(symbols[i])) return false;
    return true;
}

} // namespace

bool peek_header(const uint8_t *data, size_t len, MsgType &type)
{
    if (!data || len < sizeof(Header)) return false;
    Header h;
    std::memcpy(&h, data, sizeof h);
    if (h.version != kProtocolVersion || h.length != len) return false;
    switch (h.type)
    {
    case static_cast<uint8_t>(MsgType::SubscribeRequest):
    case static_cast<uint8_t>(MsgType::UnsubscribeRequest):
    case static_cast<uint8_t>(MsgType::SubscribeAck):
    case static_cast<uint8_t>(MsgType::Heartbeat):
    case static_cast<uint8_t>(MsgType::L2Book):
    case static_cast<uint8_t>(MsgType::Trade):
        type = static_cast<MsgType>(h.type);
        return true;
    default:
        return false;
    }
}

bool decode(const uint8_t *data, size_t len, SubscribeRequest &out)
{
    SubscribeRequest m;
    if (!check_fixed(data, len, MsgType::SubscribeRequest, m)) return false;
    if (m.symbol_count == 0 || m.symbol_count > kMaxSymbolsPerRequest) return false;
    if (!get_channel(m.reply_channel)) return false;
    if (!symbols_ok(m.symbols, m.symbol_count)) return false;
    out = m;
    return true;
}

bool decode(const uint8_t *data, size_t len, UnsubscribeRequest &out)
{
    UnsubscribeRequest m;
    if (!check_fixed(data, len, MsgType::UnsubscribeRequest, m)) return false;
    if (m.symbol_count > kMaxSymbolsPerRequest) return false;
    if (!symbols_ok(m.symbols, m.symbol_count)) return false;
    out = m;
    return true;
}

bool decode(const uint8_t *data, size_t len, SubscribeAck &out)
{
    SubscribeAck m;
    if (!check_fixed(data, len, MsgType::SubscribeAck, m)) return false;
    if (m.result_count > kMaxSymbolsPerRequest) return false;
    out = m;
    return true;
}

bool decode(const uint8_t *data, size_t len, Heartbeat &out)
{
    return check_fixed(data, len, MsgType::Heartbeat, out);
}

bool decode(const uint8_t *data, size_t len, Trade &out)
{
    return check_fixed(data, len, MsgType::Trade, out);
}

bool decode_l2(const uint8_t *data, size_t len, L2Book &head, BookLevel *bids, BookLevel *asks)
{
    MsgType t;
    if (!peek_header(data, len, t) || t != MsgType::L2Book || len < sizeof(L2Book)) return false;
    L2Book h;
    std::memcpy(&h, data, sizeof h);
    if (h.bid_count > kMaxL2Depth || h.ask_count > kMaxL2Depth) return false;
    if (len != sizeof(L2Book) + (static_cast<size_t>(h.bid_count) + h.ask_count) * sizeof(BookLevel)) return false;
    const uint8_t *p = data + sizeof(L2Book);
    if (h.bid_count) std::memcpy(bids, p, h.bid_count * sizeof(BookLevel));
    p += h.bid_count * sizeof(BookLevel);
    if (h.ask_count) std::memcpy(asks, p, h.ask_count * sizeof(BookLevel));
    head = h;
    return true;
}

size_t encode(const SubscribeRequest &m, uint8_t *out)   { SubscribeRequest c = m;   c.h = make_header(MsgType::SubscribeRequest, sizeof c);   return put(c, out); }
size_t encode(const UnsubscribeRequest &m, uint8_t *out) { UnsubscribeRequest c = m; c.h = make_header(MsgType::UnsubscribeRequest, sizeof c); return put(c, out); }
size_t encode(const SubscribeAck &m, uint8_t *out)       { SubscribeAck c = m;       c.h = make_header(MsgType::SubscribeAck, sizeof c);       return put(c, out); }
size_t encode(const Heartbeat &m, uint8_t *out)          { Heartbeat c = m;          c.h = make_header(MsgType::Heartbeat, sizeof c);          return put(c, out); }
size_t encode(const Trade &m, uint8_t *out)              { Trade c = m;              c.h = make_header(MsgType::Trade, sizeof c);              return put(c, out); }

size_t encode_l2(const L2Book &head, const BookLevel *bids, const BookLevel *asks, uint8_t *out)
{
    L2Book h = head;
    // The arrays hold at most kMaxL2Depth levels each; never read past them whatever the caller says.
    if (h.bid_count > kMaxL2Depth) h.bid_count = kMaxL2Depth;
    if (h.ask_count > kMaxL2Depth) h.ask_count = kMaxL2Depth;
    const size_t len = sizeof(L2Book) + (static_cast<size_t>(h.bid_count) + h.ask_count) * sizeof(BookLevel);
    h.h = make_header(MsgType::L2Book, len);
    std::memcpy(out, &h, sizeof h);
    uint8_t *p = out + sizeof h;
    if (h.bid_count) std::memcpy(p, bids, h.bid_count * sizeof(BookLevel));
    p += h.bid_count * sizeof(BookLevel);
    if (h.ask_count) std::memcpy(p, asks, h.ask_count * sizeof(BookLevel));
    return len;
}

bool set_symbol(char (&dst)[kSymbolLen], std::string_view s)
{
    if (s.empty() || s.size() > kSymbolLen) return false;
    for (char c : s)
        if (c <= ' ' || c > '~') return false;
    std::memset(dst, ' ', kSymbolLen);
    std::memcpy(dst, s.data(), s.size());
    return true;
}

std::optional<std::string> get_symbol(const char (&src)[kSymbolLen])
{
    size_t n = 0;
    while (n < kSymbolLen && src[n] != '\0' && src[n] != ' ') ++n;
    if (n == 0) return std::nullopt;
    for (size_t i = 0; i < n; ++i)
        if (src[i] < '!' || src[i] > '~') return std::nullopt;
    for (size_t i = n; i < kSymbolLen; ++i)                 // only padding may follow
        if (src[i] != '\0' && src[i] != ' ') return std::nullopt;
    return std::string(src, n);
}

bool set_channel(char (&dst)[kMaxChannelLen], std::string_view s)
{
    if (s.empty() || s.size() >= kMaxChannelLen) return false;
    std::memset(dst, 0, kMaxChannelLen);
    std::memcpy(dst, s.data(), s.size());
    return true;
}

std::optional<std::string_view> get_channel(const char (&src)[kMaxChannelLen])
{
    const void *nul = std::memchr(src, '\0', kMaxChannelLen);
    if (!nul) return std::nullopt;
    const size_t n = static_cast<const char *>(nul) - src;
    if (n == 0) return std::nullopt;
    return std::string_view(src, n);
}

} // namespace feed_handler::messaging

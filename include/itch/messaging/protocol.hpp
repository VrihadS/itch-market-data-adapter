#pragma once
// Wire protocol between the market data server and its clients.
//
//   client -> server  (control stream)   SubscribeRequest, UnsubscribeRequest, Heartbeat
//   server -> client  (reply stream)     SubscribeAck, L2Book, Trade, Heartbeat
//
// All integers are little-endian, structs are packed, every message starts with a 4-byte
// Header whose `length` is the total message length. Decoding is strict: a message whose
// length, version, type or counts do not check out is rejected before any field is used.

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

#include "itch/book/book_listener.hpp"

namespace feed_handler::messaging {

static_assert(std::endian::native == std::endian::little, "wire format is little-endian");

constexpr uint8_t kProtocolVersion       = 1;
constexpr size_t  kSymbolLen             = 8;    // ITCH symbol field width
constexpr size_t  kMaxSymbolsPerRequest  = 32;
constexpr size_t  kMaxChannelLen         = 96;   // includes the terminating NUL
constexpr uint8_t kMaxL2Depth            = 10;

// What a client can ask for, per instrument. Values equal the book's interest flags.
constexpr uint8_t kFeatureTimeAndSales = kInterestTrades;   // prints
constexpr uint8_t kFeatureL2Book       = kInterestBook;     // top-of-book depth, up to kMaxL2Depth
constexpr uint8_t kKnownFeatures       = kFeatureTimeAndSales | kFeatureL2Book;

enum class MsgType : uint8_t {
    SubscribeRequest   = 1,
    UnsubscribeRequest = 2,
    SubscribeAck       = 3,
    Heartbeat          = 4,
    L2Book             = 10,
    Trade              = 11,
};

enum class Status : uint8_t {
    Ok                   = 0,
    UnknownSymbol        = 1,
    BadDepth             = 2,   // L2 requested with depth outside 1..kMaxL2Depth
    BadFeatures          = 3,   // no feature, or an unknown feature bit
    BadChannel           = 4,   // reply channel malformed or not permitted
    TooManyClients       = 5,
    TooManySubscriptions = 6,
    BadRequest           = 7,   // inconsistent fields (e.g. no symbols)
    ChannelInUse         = 8,   // IPC / unicast reply channel already owned by another client
    NotSubscribed        = 9,
    Unavailable          = 10,  // gateway only: the market data server could not be reached
};
const char *to_string(Status s);

#pragma pack(push, 1)

struct Header {
    uint16_t length;    // total message length including this header
    uint8_t  version;
    uint8_t  type;      // MsgType
};

struct SubscribeRequest {
    Header   h;
    uint64_t client_id;
    uint32_t request_id;
    uint32_t reply_stream_id;
    uint8_t  features;        // kFeatureTimeAndSales | kFeatureL2Book (replaces earlier request for these symbols)
    uint8_t  depth;           // 1..kMaxL2Depth when kFeatureL2Book is set, else 0
    uint8_t  symbol_count;    // 1..kMaxSymbolsPerRequest
    uint8_t  reserved;
    char     reply_channel[kMaxChannelLen];                 // NUL-terminated Aeron channel
    char     symbols[kMaxSymbolsPerRequest][kSymbolLen];    // space/NUL padded
};

struct UnsubscribeRequest {
    Header   h;
    uint64_t client_id;
    uint32_t request_id;
    uint8_t  features;        // features to drop for the listed symbols
    uint8_t  symbol_count;    // 0 = drop everything and disconnect this client
    uint16_t reserved;
    char     symbols[kMaxSymbolsPerRequest][kSymbolLen];
};

struct SymbolResult {
    uint16_t locate;          // ITCH stock locate; 0 if unknown
    uint8_t  status;          // Status
    uint8_t  reserved;
};

struct SubscribeAck {
    Header       h;
    uint64_t     client_id;
    uint32_t     request_id;
    uint8_t      status;          // request-level Status; per-symbol results valid when Ok
    uint8_t      result_count;
    uint16_t     reserved;
    SymbolResult results[kMaxSymbolsPerRequest];
};

struct Heartbeat {
    Header   h;
    uint64_t client_id;       // client heartbeats: sender; server heartbeats: addressed client (0 if shared)
    uint64_t send_ts_ns;      // wall clock, ns since epoch
    uint8_t  from_server;
    uint8_t  reserved[3];
};

struct BookLevel {
    int32_t  price;           // 4 implied decimals
    uint32_t orders;
    uint64_t shares;
};

// Followed by bid_count + ask_count BookLevel entries: bids best-first, then asks best-first.
struct L2Book {
    Header   h;
    uint16_t locate;
    uint8_t  bid_count;       // <= kMaxL2Depth
    uint8_t  ask_count;       // <= kMaxL2Depth
    uint8_t  snapshot;        // 1 = sent in response to a subscription, 0 = update
    uint8_t  reserved[3];
    uint64_t sequence;        // per (client sink, locate); gaps mean lost updates
    uint64_t exchange_ts_ns;  // ITCH timestamp, ns since midnight
    uint64_t send_ts_ns;      // wall clock when the server sent it, ns since epoch
};

struct Trade {
    Header   h;
    uint16_t locate;
    char     kind;            // 'E','C','P','Q','B' (see TradeEvent)
    char     side;            // 'B'/'S' resting side, or 0
    char     cross_type;      // 'O','C','H','I' for 'Q', else 0
    uint8_t  reserved[3];
    uint64_t sequence;        // per (client sink, locate)
    uint64_t exchange_ts_ns;
    uint64_t send_ts_ns;
    int32_t  price;
    uint32_t reserved2;
    uint64_t shares;
    uint64_t match_number;
};

#pragma pack(pop)

static_assert(sizeof(Header) == 4);
static_assert(sizeof(SubscribeRequest) == 376);
static_assert(sizeof(UnsubscribeRequest) == 276);
static_assert(sizeof(SubscribeAck) == 148);
static_assert(sizeof(Heartbeat) == 24);
static_assert(sizeof(BookLevel) == 16);
static_assert(sizeof(L2Book) == 36);
static_assert(sizeof(Trade) == 60);
constexpr size_t kMaxL2MessageSize = sizeof(L2Book) + 2 * kMaxL2Depth * sizeof(BookLevel);
constexpr size_t kMaxMessageSize   = sizeof(SubscribeRequest);   // largest message of any type
static_assert(kMaxL2MessageSize <= kMaxMessageSize);

// ── header helpers ──────────────────────────────────────────────────
inline Header make_header(MsgType t, size_t length) {
    return Header{static_cast<uint16_t>(length), kProtocolVersion, static_cast<uint8_t>(t)};
}

// Reads and checks the header of an incoming message. On success `type` is set; the caller
// must still use the matching decode_* function, which validates the body.
bool peek_header(const uint8_t *data, size_t len, MsgType &type);

// ── decoding (copy out; reject anything malformed) ──────────────────
bool decode(const uint8_t *data, size_t len, SubscribeRequest &out);
bool decode(const uint8_t *data, size_t len, UnsubscribeRequest &out);
bool decode(const uint8_t *data, size_t len, SubscribeAck &out);
bool decode(const uint8_t *data, size_t len, Heartbeat &out);
bool decode(const uint8_t *data, size_t len, Trade &out);

// L2 messages are variable length: the levels are copied into the caller's arrays.
bool decode_l2(const uint8_t *data, size_t len, L2Book &head, BookLevel *bids /*[kMaxL2Depth]*/,
               BookLevel *asks /*[kMaxL2Depth]*/);

// ── encoding ────────────────────────────────────────────────────────
// Each returns the encoded length. `out` must hold at least kMaxMessageSize bytes.
size_t encode(const SubscribeRequest &m, uint8_t *out);
size_t encode(const UnsubscribeRequest &m, uint8_t *out);
size_t encode(const SubscribeAck &m, uint8_t *out);
size_t encode(const Heartbeat &m, uint8_t *out);
size_t encode(const Trade &m, uint8_t *out);
// bid_count / ask_count are clamped to kMaxL2Depth; `out` needs kMaxL2MessageSize bytes.
size_t encode_l2(const L2Book &head, const BookLevel *bids, const BookLevel *asks, uint8_t *out);

// ── field helpers ───────────────────────────────────────────────────
// Symbol field <-> string. Accepts 1..8 printable, non-space ASCII characters.
bool                       set_symbol(char (&dst)[kSymbolLen], std::string_view symbol);
std::optional<std::string> get_symbol(const char (&src)[kSymbolLen]);

// Channel field <-> string. get_channel requires a NUL inside the field.
bool                           set_channel(char (&dst)[kMaxChannelLen], std::string_view channel);
std::optional<std::string_view> get_channel(const char (&src)[kMaxChannelLen]);

} // namespace feed_handler::messaging

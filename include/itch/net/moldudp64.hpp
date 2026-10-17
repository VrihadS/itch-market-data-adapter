#pragma once
// MoldUDP64 packet layer (Nasdaq MoldUDP64 Protocol Specification v1.00, docs/spec/MoldUDP64.pdf).
//
//   Downstream packet = Header(20) + Message Block*          Request packet = Header(20)
//   Header:        Session[10] | Sequence Number[8] | Message Count[2]     (numbers big-endian)
//   Message Block: Message Length[2] | Message Data[length]                (length may be 0)
//   Message Count 0 = heartbeat, 0xFFFF = end of session; both carry the NEXT expected sequence number.
//
// This layer knows nothing about ITCH. parse_packet() validates a WHOLE packet (every block inside
// the packet, no bytes left over) before any block is exposed, so callers never see a half-trusted
// packet: a packet whose blocks do not add up is rejected as a unit.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

#include "itch/byteorder.hpp"

namespace feed_handler::mold {

constexpr size_t   kHeaderSize         = 20;
constexpr size_t   kSessionLen         = 10;
constexpr uint16_t kHeartbeatCount     = 0;
constexpr uint16_t kEndOfSessionCount  = 0xFFFF;
constexpr size_t   kBlockOverhead      = 2;
constexpr size_t   kDefaultMaxPayload  = 1400;   // bytes of UDP payload per packet when we are the sender

struct Session {
    char id[kSessionLen];

    Session() { std::memset(id, ' ', sizeof id); }
    explicit Session(std::string_view s) {
        std::memset(id, ' ', sizeof id);
        std::memcpy(id, s.data(), s.size() < sizeof id ? s.size() : sizeof id);
    }
    bool operator==(const Session &o) const { return std::memcmp(id, o.id, sizeof id) == 0; }
    bool operator!=(const Session &o) const { return !(*this == o); }
    std::string str() const {   // trailing padding removed
        size_t n = sizeof id;
        while (n > 0 && (id[n - 1] == ' ' || id[n - 1] == '\0')) --n;
        return std::string(id, n);
    }
};

enum class PacketKind : uint8_t { Data, Heartbeat, EndOfSession };

enum class ParseError : uint8_t {
    None,
    TooShort,        // shorter than the 20-byte header
    Truncated,       // a block's length runs past the end of the packet
    TrailingBytes,   // bytes left after `count` blocks, or a payload on a heartbeat / end-of-session
    MissingBlocks,   // fewer than `count` blocks in the payload
};

struct Header {
    Session  session;
    uint64_t sequence = 0;   // data: first message's sequence; heartbeat / end of session: next expected
    uint16_t count    = 0;
};

// A validated packet. `payload` points into the caller's buffer, which must outlive this object.
struct Packet {
    Header         header;
    PacketKind     kind = PacketKind::Data;
    const uint8_t *payload = nullptr;
    size_t         payload_len = 0;

    uint64_t first_sequence() const { return header.sequence; }
    // One past the last message (data), or the advertised next sequence (heartbeat / end of session).
    uint64_t end_sequence() const {
        return kind == PacketKind::Data ? header.sequence + header.count : header.sequence;
    }
    uint16_t message_count() const { return kind == PacketKind::Data ? header.count : 0; }
};

// Iterates the blocks of a packet that parse_packet() accepted (so it cannot overrun).
class BlockIterator {
public:
    explicit BlockIterator(const Packet &p) : cur_(p.payload), left_(p.message_count()) {}
    // Yields the next message (data may be null when length is 0). Returns false after the last.
    bool next(const uint8_t *&data, uint16_t &length) {
        if (left_ == 0) return false;
        length = itch::read_be16(cur_);
        data = length ? cur_ + kBlockOverhead : nullptr;
        cur_ += kBlockOverhead + length;
        --left_;
        return true;
    }
private:
    const uint8_t *cur_;
    uint32_t left_;
};

inline ParseError parse_packet(const uint8_t *data, size_t len, Packet &out) {
    if (!data || len < kHeaderSize) return ParseError::TooShort;
    Header h;
    std::memcpy(h.session.id, data, kSessionLen);
    h.sequence = itch::read_be64(data + 10);
    h.count    = itch::read_be16(data + 18);

    Packet p;
    p.header = h;
    p.payload = data + kHeaderSize;
    p.payload_len = len - kHeaderSize;
    if (h.count == kHeartbeatCount || h.count == kEndOfSessionCount) {
        if (p.payload_len != 0) return ParseError::TrailingBytes;
        p.kind = h.count == kHeartbeatCount ? PacketKind::Heartbeat : PacketKind::EndOfSession;
        out = p;
        return ParseError::None;
    }
    p.kind = PacketKind::Data;
    size_t off = 0;
    for (uint32_t i = 0; i < h.count; ++i) {
        if (p.payload_len - off < kBlockOverhead) return ParseError::MissingBlocks;
        const size_t mlen = itch::read_be16(p.payload + off);
        if (p.payload_len - off - kBlockOverhead < mlen) return ParseError::Truncated;
        off += kBlockOverhead + mlen;
    }
    if (off != p.payload_len) return ParseError::TrailingBytes;
    out = p;
    return ParseError::None;
}

// ── building ────────────────────────────────────────────────────────────────

inline size_t write_header(uint8_t *out, const Session &s, uint64_t sequence, uint16_t count) {
    std::memcpy(out, s.id, kSessionLen);
    itch::write_be64(out + 10, sequence);
    itch::write_be16(out + 18, count);
    return kHeaderSize;
}

// A request for retransmission has the same 20-byte layout as a header.
inline size_t build_request(uint8_t *out, const Session &s, uint64_t first_sequence, uint16_t count) {
    return write_header(out, s, first_sequence, count);
}
inline size_t build_heartbeat(uint8_t *out, const Session &s, uint64_t next_sequence) {
    return write_header(out, s, next_sequence, kHeartbeatCount);
}
inline size_t build_end_of_session(uint8_t *out, const Session &s, uint64_t next_sequence) {
    return write_header(out, s, next_sequence, kEndOfSessionCount);
}

// Packs messages into one downstream packet up to a payload budget.
class PacketBuilder {
public:
    PacketBuilder(uint8_t *buf, size_t capacity, size_t max_packet_bytes = kDefaultMaxPayload)
        : buf_(buf), limit_(max_packet_bytes < capacity ? max_packet_bytes : capacity) {}

    void begin(const Session &s, uint64_t first_sequence) {
        session_ = s; first_ = first_sequence; count_ = 0; len_ = kHeaderSize;
    }
    // False if the message does not fit (the packet is left unchanged). A message that could never
    // fit even in an empty packet also returns false; check `empty()` to tell the cases apart.
    bool add(const uint8_t *msg, size_t n) {
        if (n > 0xFFFF || count_ == 0xFFFE || len_ + kBlockOverhead + n > limit_) return false;
        itch::write_be16(buf_ + len_, static_cast<uint16_t>(n));
        if (n) std::memcpy(buf_ + len_ + kBlockOverhead, msg, n);
        len_ += kBlockOverhead + n;
        ++count_;
        return true;
    }
    bool empty() const { return count_ == 0; }
    uint16_t count() const { return count_; }
    uint64_t next_sequence() const { return first_ + count_; }
    // Finishes the packet and returns its length.
    size_t finish() { write_header(buf_, session_, first_, count_); return len_; }
    const uint8_t *data() const { return buf_; }
private:
    uint8_t *buf_;
    size_t   limit_;
    Session  session_;
    uint64_t first_ = 0;
    uint16_t count_ = 0;
    size_t   len_ = kHeaderSize;
};

}  // namespace feed_handler::mold

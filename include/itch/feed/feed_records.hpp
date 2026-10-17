#pragma once
// What crosses from the network thread to the book thread: records in an SpscRing.
//
//   kRecMessage       u64 sequence | message bytes          (one ITCH message, in sequence order)
//   kRecSessionStart  session[10] | catching_up u8 | rebuild u8 | u64 first_sequence   -> reset the books
//   kRecCaughtUp      u64 next_sequence                     (catch-up finished: now live)
//   kRecEndOfSession  session[10] | pad | u64 next_sequence
//   kRecStale         u64 expected | u64 highest_seen       (the books can no longer be trusted)
//
// Control records may use the ring's reserved space, so they get through even when data has filled it.

#include <cstdint>
#include <cstring>

#include "itch/feed/feed_session.hpp"
#include "itch/feed/spsc_ring.hpp"

namespace feed_handler::feed {

enum RecordType : uint32_t {
    kRecMessage      = 1,
    kRecSessionStart = 2,
    kRecCaughtUp     = 3,
    kRecEndOfSession = 4,
    kRecStale        = 5,
};

constexpr size_t kSessionStartBytes = 24;

// Writes FeedSession output into a ring. Producer side: network thread only.
class RingSink final : public IFeedSink {
public:
    explicit RingSink(SpscRing &ring) : ring_(ring) {}

    bool on_message(uint64_t seq, const uint8_t *data, uint16_t len) override {
        uint8_t *p = ring_.claim(kRecMessage, 8 + len, /*control=*/false);
        if (!p) { data_full_.add(); return false; }
        std::memcpy(p, &seq, 8);
        std::memcpy(p + 8, data, len);
        ring_.publish();
        return true;
    }
    bool on_session_start(const mold::Session &s, uint64_t first, bool catching_up, bool rebuild) override {
        uint8_t *p = ring_.claim(kRecSessionStart, kSessionStartBytes, true);
        if (!p) { control_full_.add(); return false; }
        std::memset(p, 0, kSessionStartBytes);
        std::memcpy(p, s.id, mold::kSessionLen);
        p[10] = catching_up; p[11] = rebuild;
        std::memcpy(p + 16, &first, 8);
        ring_.publish();
        return true;
    }
    bool on_caught_up(uint64_t next) override { return control(kRecCaughtUp, nullptr, next, 0); }
    bool on_end_of_session(const mold::Session &s, uint64_t next) override { return control(kRecEndOfSession, &s, next, 0); }
    bool on_stale(StaleReason, uint64_t expected, uint64_t highest) override { return control(kRecStale, nullptr, expected, highest); }

    uint64_t data_full() const { return data_full_.get(); }        // times the ring had no room for a message
    uint64_t control_full() const { return control_full_.get(); }

private:
    bool control(uint32_t type, const mold::Session *s, uint64_t a, uint64_t b) {
        uint8_t *p = ring_.claim(type, 24, true);
        if (!p) { control_full_.add(); return false; }
        std::memset(p, 0, 24);
        if (s) std::memcpy(p, s->id, mold::kSessionLen);
        std::memcpy(p + 8, &a, 8);
        std::memcpy(p + 16, &b, 8);
        ring_.publish();
        return true;
    }
    SpscRing &ring_;
    Counter data_full_, control_full_;
};

}  // namespace feed_handler::feed

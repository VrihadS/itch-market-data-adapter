#pragma once
// The book-thread half of the feed adapter: drains the ring and applies it to the BookManager.
//
//   SessionStart  -> books.reset(); if the session is catching up, the book listener is detached so
//                    subscribers are not flooded with a replay of history
//   Message       -> ITCH decode -> BookManager
//   CaughtUp      -> listener re-attached (and on_live fires, e.g. to resend L2 snapshots)
//   EndOfSession / Stale -> status + hook
//
// Call poll() from the thread that owns the BookManager (and the MarketDataServer, if any).

#include <atomic>
#include <functional>

#include "itch/book/book_manager.hpp"
#include "itch/feed/feed_records.hpp"
#include "itch/protocol/ndaq_itch_decoder.h"

namespace feed_handler::feed {

class FeedConsumer {
public:
    enum class Mode : int { Idle, CatchingUp, Live, Stale, Ended };

    struct Hooks {
        std::function<void(bool catching_up, bool rebuild)> on_session_start;
        std::function<void()> on_live;            // live (initially, or after catching up): books are current
        std::function<void()> on_end_of_session;
        std::function<void()> on_stale;           // the books are no longer reliable
    };

    FeedConsumer(SpscRing &ring, BookManager &books) : ring_(ring), books_(books) {}
    void set_hooks(Hooks h) { hooks_ = std::move(h); }

    // Applies up to `max_records` records. Returns how many were applied.
    int poll(int max_records = 4096);

    Mode mode() const { return static_cast<Mode>(mode_.load(std::memory_order_relaxed)); }
    uint64_t messages() const { return messages_.load(std::memory_order_relaxed); }
    uint64_t decode_failures() const { return decode_failures_.load(std::memory_order_relaxed); }
    uint64_t last_sequence() const { return last_seq_.load(std::memory_order_relaxed); }
    uint64_t session_starts() const { return session_starts_.load(std::memory_order_relaxed); }

private:
    void set_mode(Mode m) { mode_.store(static_cast<int>(m), std::memory_order_relaxed); }

    SpscRing &ring_;
    BookManager &books_;
    NasdaqItchDecoder decoder_;
    Hooks hooks_;
    BookListener *detached_listener_ = nullptr;
    bool detached_ = false;
    std::atomic<int> mode_{static_cast<int>(Mode::Idle)};
    std::atomic<uint64_t> messages_{0}, decode_failures_{0}, last_seq_{0}, session_starts_{0};
};

}  // namespace feed_handler::feed

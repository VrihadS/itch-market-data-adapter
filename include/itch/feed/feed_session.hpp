#pragma once
// MoldUDP64 receive logic: sequencing, A/B line arbitration, gap recovery, session lifecycle.
//
// One FeedSession consumes packets from three sources, all carrying the same sequence-numbered stream:
//   Line::A, Line::B   the two redundant multicast lines (whichever delivers a sequence number first wins;
//                      the other copy is dropped as a duplicate)
//   Line::Rerequest    unicast replies from the re-request server
// and hands each message to an IFeedSink exactly once, in sequence order.
//
//   gap on A  ->  usually B already has it (arbitration)  ->  else wait gap_wait, then ask the re-request
//   server for the missing range (one request in flight, retried, pipelined as replies arrive)  ->  if the
//   server never answers, go Stale: the books must not be trusted until rebuilt.
//
// Joining mid-day: ITCH has no snapshot, so a book can only be built by replaying from sequence 1. With
// join_from_start (the default) a late joiner starts at 1 and CATCHES UP through re-requests while live
// packets keep arriving; the sink is told it is catching up so it can hold back publishing.
//
// Deterministic by design: no sockets, threads or clocks inside. The caller passes `now_ns` and calls
// tick() regularly. NOT thread-safe: drive it from one thread (the network thread). Counters are
// readable from other threads.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <vector>

#include "itch/net/moldudp64.hpp"

namespace feed_handler::feed {

enum class Line : uint8_t { A = 0, B = 1, Rerequest = 2 };
constexpr size_t kLineCount = 3;
const char *to_string(Line l);

enum class State : uint8_t {
    WaitingForSession,   // nothing received yet
    Live,                // in step with the feed
    CatchingUp,          // behind the feed and replaying from the re-request server
    Stale,               // a gap could not be filled: messages after it were NOT delivered
    Ended,               // end of session reached and every message delivered
};
const char *to_string(State s);

enum class StaleReason : uint8_t { GapUnrecoverable };

struct FeedConfig {
    // Start every session at sequence 1 (needed to build order books). If false, start wherever the
    // first packet says (useful for a consumer that only wants the live tail).
    bool join_from_start = true;

    // How long to give the other line to fill a gap before asking the re-request server.
    uint64_t gap_wait_ns = 5'000'000;
    // Retry timing adapts to the server: the timeout is srtt + 4 * rttvar measured from replies (as in TCP), doubles
    // on each retry of the same request, and is clamped to [min_request_timeout_ns, request_timeout_ns]. Until the
    // first reply has been timed it is request_timeout_ns.
    uint64_t request_timeout_ns = 200'000'000;     // initial and maximum
    uint64_t min_request_timeout_ns = 3'000'000;
    uint32_t max_request_attempts = 5;             // for the same position, then Stale
    uint32_t max_request_messages = 1000;          // upper bound per request; the size adapts to what one reply holds
    uint32_t request_window = 8;                   // requests in flight at once (catching up is limited by round trips)

    size_t max_buffered_packets = 4096;            // out-of-order packets held; beyond this the farthest are dropped
    size_t max_buffered_bytes = 16u << 20;         //   (and later re-requested), so the limit never loses data

    uint64_t line_timeout_ns = 3'000'000'000ull;   // no packet on a line for this long: the line is down
    uint64_t session_switch_timeout_ns = 5'000'000'000ull;   // current session silent this long: accept a different one
    uint64_t max_sequence = 1ull << 48;            // sanity bound; packets beyond it are treated as corrupt

    // A hole that makes no progress for gap_wait + request_timeout * (max_request_attempts + 1) is declared unrecoverable
    // whatever the cause (server silent, no server configured, requests that cannot be sent).

    // After going Stale, wait this long and then rebuild from sequence 1 by itself (0 = never; the
    // application can call rebuild()).
    uint64_t auto_rebuild_after_ns = 0;
};

// Where messages and session events go. Every method returns false if the destination has no room right
// now; the session then keeps what it could not deliver and tries again, so nothing is lost or reordered.
class IFeedSink {
public:
    virtual ~IFeedSink() = default;
    virtual bool on_session_start(const mold::Session &s, uint64_t first_sequence, bool catching_up, bool rebuild) = 0;
    virtual bool on_message(uint64_t sequence, const uint8_t *data, uint16_t length) = 0;
    virtual bool on_caught_up(uint64_t next_sequence) = 0;
    virtual bool on_end_of_session(const mold::Session &s, uint64_t next_sequence) = 0;
    virtual bool on_stale(StaleReason reason, uint64_t expected, uint64_t highest_seen) = 0;
};

class IRerequestSender {
public:
    virtual ~IRerequestSender() = default;
    virtual bool send_request(const mold::Session &s, uint64_t first_sequence, uint16_t count) = 0;
};

// Single-writer counters that other threads may read (no locked instructions on the write side).
class Counter {
public:
    void add(uint64_t n = 1) { v_.store(v_.load(std::memory_order_relaxed) + n, std::memory_order_relaxed); }
    uint64_t get() const { return v_.load(std::memory_order_relaxed); }
private:
    std::atomic<uint64_t> v_{0};
};

struct LineStats {
    uint64_t packets = 0, bytes = 0, malformed = 0;
    uint64_t heartbeats = 0, end_of_session = 0;
    uint64_t data_packets = 0, duplicate_packets = 0;
    uint64_t messages_won = 0;     // messages first delivered from this line
    uint64_t outages = 0;
    bool     up = false;
};

struct FeedStats {
    LineStats line[kLineCount];
    State    state = State::WaitingForSession;
    uint64_t next_sequence = 0, highest_seen = 0;
    uint64_t delivered = 0, zero_length = 0, duplicate_messages = 0;
    uint64_t gaps_detected = 0, gaps_closed = 0;
    uint64_t requests_sent = 0, request_retries = 0, request_send_failures = 0, recovered_messages = 0;
    uint64_t buffer_drops = 0, sink_blocked = 0;
    uint64_t session_starts = 0, sessions_ended = 0, session_mismatch_packets = 0;
    uint64_t stale_events = 0, rebuilds = 0, caught_up_events = 0, out_of_range_packets = 0;
};

class FeedSession {
public:
    FeedSession(FeedConfig cfg, IFeedSink &sink, IRerequestSender &rerequest);

    // Feed one datagram from `line`. `data` is only read during the call.
    void on_packet(Line line, const uint8_t *data, size_t len, uint64_t now_ns);

    // Timers: gap waits, request retries, line liveness, retrying blocked deliveries. Call every few ms.
    void tick(uint64_t now_ns);

    // Drop everything and replay the session from sequence 1 (e.g. after Stale).
    void rebuild(uint64_t now_ns);

    State    state() const { return state_.load(std::memory_order_relaxed); }
    bool     line_up(Line l) const { return line_up_[static_cast<size_t>(l)].load(std::memory_order_relaxed); }
    FeedStats stats() const;                 // safe from any thread (individual counters, not one atomic snapshot)
    uint64_t next_sequence() const { return next_pub_.load(std::memory_order_relaxed); }
    mold::Session session() const { return session_; }   // network thread only

private:
    struct Buffered {
        Line line;
        uint64_t end;
        std::vector<uint8_t> bytes;
    };
    struct Req {                                   // a retransmission request in flight
        uint64_t first, count, sent_ns;
        uint32_t attempts;
    };
    enum class EventKind : uint8_t { SessionStart, CaughtUp, EndOfSession, Stale };
    struct Event {
        EventKind kind;
        mold::Session session;
        uint64_t a = 0, b = 0;
        bool flag1 = false, flag2 = false;
    };

    bool check_session(const mold::Packet &p, uint64_t now);
    void begin_session(const mold::Packet &p, uint64_t now, bool rebuild);
    void handle_data(Line line, const uint8_t *data, size_t len, const mold::Packet &p, uint64_t now);
    uint64_t deliver_from(Line line, const mold::Packet &p, uint64_t now);
    void buffer_packet(Line line, const uint8_t *data, size_t len, const mold::Packet &p);
    void drain_buffer(uint64_t now);
    void advance(uint64_t now);
    void maybe_request(uint64_t now);
    void fill_window(uint64_t now);
    void note_reply(uint64_t first, uint64_t count, uint64_t now);
    uint64_t timeout_for(const Req &r) const;
    uint64_t covered_end(uint64_t seq) const;
    void go_stale(uint64_t now);
    void note_high(uint64_t seq);
    void set_state(State s) { state_.store(s, std::memory_order_relaxed); }
    void emit(const Event &e);
    bool try_emit(const Event &e);
    bool flush_events();
    bool gap_open() const { return highest_ > next_; }

    FeedConfig cfg_;
    IFeedSink &sink_;
    IRerequestSender &rerequest_;

    mold::Session session_;
    bool have_session_ = false;
    std::atomic<State> state_{State::WaitingForSession};
    uint64_t next_ = 0;                    // next sequence to deliver
    std::atomic<uint64_t> next_pub_{0};
    uint64_t highest_ = 0;                 // highest "next expected" seen on any line / packet end
    bool eos_seen_ = false;
    uint64_t eos_next_ = 0;
    uint64_t last_session_rx_ns_ = 0;
    uint64_t stale_since_ns_ = 0;

    std::map<uint64_t, Buffered> buffer_;
    size_t buffered_bytes_ = 0;
    bool gap_active_ = false;
    uint64_t gap_since_ns_ = 0;
    uint64_t stall_pos_ = 0, stall_since_ns_ = 0;   // the position blocking delivery, and since when
    std::vector<Req> win_;                 // sorted by first
    uint64_t srtt_ns_ = 0, rttvar_ns_ = 0, rto_ns_ = 0;   // reply timing; rto_ns_ == 0 until the first sample
    uint32_t chunk_ = 64;                  // messages per request; learned from the replies
    bool sink_blocked_ = false;
    std::vector<Event> pending_events_;    // control events the sink has not accepted yet, in order

    struct LineC {
        Counter packets, bytes, malformed, heartbeats, end_of_session, data_packets, duplicate_packets, messages_won, outages;
        uint64_t last_rx_ns = 0;
        bool ever_rx = false;
    } line_[kLineCount];
    std::atomic<bool> line_up_[kLineCount];
    uint64_t start_ns_ = 0;
    bool started_ = false;

    Counter delivered_, zero_length_, duplicate_messages_, gaps_detected_, gaps_closed_, requests_sent_, request_retries_,
        request_send_failures_, recovered_messages_, buffer_drops_, sink_blocked_c_, session_starts_, sessions_ended_,
        session_mismatch_, stale_events_, rebuilds_, caught_up_, out_of_range_;
    std::atomic<uint64_t> highest_pub_{0};
};

}  // namespace feed_handler::feed

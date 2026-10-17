#pragma once
// The sending side of MoldUDP64, for tests and for the itch_mold_sender tool: a sequence-numbered message
// log, a packetizer, a fault injector, and a re-request server. Nothing here is on the receive path.
//
// The fault injector is pure: given a packet and its send time it returns the deliveries (possibly none,
// possibly several, each with a due time), driven by a seeded RNG, so the same scenario can run
// deterministically against FeedSession in a test and over real UDP sockets in the tool.

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "itch/net/moldudp64.hpp"

namespace feed_handler::feed {

// ── message log ──────────────────────────────────────────────────────────────

// Messages numbered from 1. Thread-safe: one thread appends while the re-request server reads.
class MessageLog {
public:
    // Appends and returns the sequence number assigned.
    uint64_t append(const uint8_t *data, size_t len);
    uint64_t next_sequence() const;     // one past the last message
    uint64_t first_sequence() const;    // oldest message still held
    // Drops messages before `seq` (simulates a re-request server with a finite retention window).
    void     trim_before(uint64_t seq);
    // Copies message `seq` into `out`; false if it is not held.
    bool     get(uint64_t seq, std::vector<uint8_t> &out) const;
    // Builds ONE downstream packet starting at `first` with as many of `count` messages as fit in
    // `max_packet_bytes`. Empty if `first` is not held or `count` is 0. Only messages below `limit`
    // (what the sender has actually sent so far) are eligible.
    std::vector<uint8_t> build_packet(const mold::Session &s, uint64_t first, uint32_t count, size_t max_packet_bytes,
                                      uint64_t limit = UINT64_MAX) const;
private:
    mutable std::mutex mu_;
    std::vector<uint8_t> arena_;
    std::vector<std::pair<uint64_t, uint32_t>> index_;   // offset, length of message (base_ + i)
    uint64_t base_ = 1;                                   // sequence of index_[0]
};

// Cuts a message stream into downstream packets of at most `max_packet_bytes`.
class Packetizer {
public:
    Packetizer(const mold::Session &s, size_t max_packet_bytes = mold::kDefaultMaxPayload)
        : session_(s), buf_(max_packet_bytes + 64), builder_(buf_.data(), buf_.size(), max_packet_bytes) { builder_.begin(session_, next_); }
    // Adds a message; if it did not fit, the finished packet is returned in `out` first and the message
    // starts the next one. Returns true if a packet was completed.
    bool add(const uint8_t *msg, size_t len, std::vector<uint8_t> &out);
    // Finishes the packet in progress (if any messages are in it).
    bool flush(std::vector<uint8_t> &out);
    uint64_t next_sequence() const { return builder_.next_sequence(); }
private:
    mold::Session session_;
    std::vector<uint8_t> buf_;
    mold::PacketBuilder builder_;
    uint64_t next_ = 1;
};

// ── fault injection ───────────────────────────────────────────────────────────

struct FaultConfig {
    double   loss = 0.0;               // probability a packet is dropped
    double   burst_start = 0.0;        // probability a loss burst starts at a packet...
    uint32_t burst_len = 0;            //   ...and drops this many consecutive packets
    double   duplicate = 0.0;          // probability a packet is delivered twice
    double   corrupt = 0.0;            // probability a packet is mangled (truncated / junk appended / count broken)
    double   reorder = 0.0;            // probability a packet is held back by an extra reorder_delay_ns
    uint64_t reorder_delay_ns = 0;
    uint64_t base_delay_ns = 0;        // fixed one-way latency of this line
    uint64_t jitter_ns = 0;            // plus uniform [0, jitter_ns]
};

class FaultInjector {
public:
    struct Delivery { uint64_t due_ns; std::vector<uint8_t> bytes; };
    FaultInjector(const FaultConfig &cfg, uint64_t seed) : cfg_(cfg), rng_(seed) {}
    // Deliveries (zero, one or two) for a packet sent at `send_ns`.
    void apply(const uint8_t *packet, size_t len, uint64_t send_ns, std::vector<Delivery> &out);
    uint64_t dropped() const { return dropped_; }
    uint64_t duplicated() const { return duplicated_; }
    uint64_t corrupted() const { return corrupted_; }
private:
    double u() { return std::uniform_real_distribution<double>(0.0, 1.0)(rng_); }
    FaultConfig cfg_;
    std::mt19937_64 rng_;
    uint32_t burst_left_ = 0;
    uint64_t dropped_ = 0, duplicated_ = 0, corrupted_ = 0;
};

// ── UDP (POSIX) ────────────────────────────────────────────────────────────────

class UdpSender {
public:
    UdpSender() = default;
    ~UdpSender();
    UdpSender(const UdpSender &) = delete;
    UdpSender &operator=(const UdpSender &) = delete;
    // `iface_ip` selects the outgoing interface for multicast destinations.
    bool open(const std::string &dest_ip, uint16_t port, const std::string &iface_ip, int ttl, std::string &error);
    bool send(const uint8_t *data, size_t len);
    void close();
private:
    int fd_ = -1;
    std::vector<uint8_t> addr_;
};

// Answers MoldUDP64 request packets: a normal downstream packet, unicast back to the requester, holding
// as many of the requested messages as fit in one packet. Requests for messages not (yet) held are ignored.
class UdpRerequestServer {
public:
    struct Options {
        size_t   max_packet_bytes = mold::kDefaultMaxPayload;
        double   drop_probability = 0.0;      // ignore this fraction of requests (exercises client retries)
        uint64_t seed = 1;
    };
    UdpRerequestServer(const mold::Session &s, const MessageLog &log, std::atomic<uint64_t> &sent_next, Options opt)
        : session_(s), log_(log), sent_next_(sent_next), opt_(opt) {}
    ~UdpRerequestServer() { stop(); }
    bool start(const std::string &bind_ip, uint16_t port, std::string &error);
    void stop();
    uint16_t port() const { return port_; }
    uint64_t served() const { return served_.load(); }
    uint64_t ignored() const { return ignored_.load(); }
    uint64_t received() const { return received_.load(); }
private:
    void run();
    mold::Session session_;
    const MessageLog &log_;
    std::atomic<uint64_t> &sent_next_;      // next sequence the sender has put on the wire
    Options opt_;
    int fd_ = -1;
    uint16_t port_ = 0;
    std::atomic<bool> stop_{false};
    std::thread thread_;
    std::atomic<uint64_t> served_{0}, ignored_{0}, received_{0};
};

// ── a complete feed: log + packetizer + two faulty lines + heartbeats + end of session ──────────────────────────

// Drives lines A and B like a MoldUDP64 transmitter. Call publish() for each message and service() often;
// deliveries that the fault injector delayed are sent when due. Not thread-safe (one driving thread); the
// MessageLog and `sent_next` may be read by a UdpRerequestServer on another thread.
class MoldFeedSender {
public:
    struct Config {
        mold::Session session{"SESSION001"};
        size_t   max_packet_bytes = mold::kDefaultMaxPayload;
        FaultConfig fault_a, fault_b;
        uint64_t seed = 1;
        uint64_t heartbeat_interval_ns = 1'000'000'000ull;
        uint64_t end_of_session_interval_ns = 100'000'000ull;   // end-of-session packets replace heartbeats
        uint64_t end_of_session_tail_ns = 2'000'000'000ull;     // ...for this long
    };
    MoldFeedSender(const Config &cfg, UdpSender &line_a, UdpSender &line_b, MessageLog &log, std::atomic<uint64_t> &sent_next);

    void publish(const uint8_t *msg, size_t len, uint64_t now_ns);   // log it, packetize, send when a packet fills
    void flush(uint64_t now_ns);                                      // send the packet in progress
    void service(uint64_t now_ns);                                    // due deliveries, heartbeats, end of session
    void end_session(uint64_t now_ns);                                // flush, then announce the end for a while
    bool finished(uint64_t now_ns) const;                             // end announced, tail over, nothing pending

    uint64_t packets_sent() const { return packets_; }
    uint64_t dropped(int line) const { return line == 0 ? fa_.dropped() : fb_.dropped(); }
    uint64_t duplicated(int line) const { return line == 0 ? fa_.duplicated() : fb_.duplicated(); }
    uint64_t corrupted(int line) const { return line == 0 ? fa_.corrupted() : fb_.corrupted(); }

private:
    struct Pending { uint64_t due_ns; uint64_t order; int line; std::vector<uint8_t> bytes; bool operator>(const Pending &o) const { return due_ns != o.due_ns ? due_ns > o.due_ns : order > o.order; } };
    void transmit(const std::vector<uint8_t> &pkt, uint64_t now_ns);
    void schedule_both(const uint8_t *data, size_t len, uint64_t now_ns);

    Config cfg_;
    UdpSender *line_[2];
    MessageLog &log_;
    std::atomic<uint64_t> &sent_next_;
    Packetizer packetizer_;
    FaultInjector fa_, fb_;
    std::vector<Pending> heap_;
    uint64_t order_ = 0, packets_ = 0;
    uint64_t last_send_ns_ = 0, last_eos_ns_ = 0, eos_started_ns_ = 0;
    bool ended_ = false;
};

}  // namespace feed_handler::feed

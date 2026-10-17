#pragma once
// Minimal transport abstraction. Everything above this (server, client) is written against
// these interfaces, so the same logic runs over Aeron (aeron_transport.hpp) or an in-process
// transport for deterministic tests (inproc_transport.hpp).
//
// Semantics follow Aeron: a publication and a subscription meet on (channel, stream id);
// every subscription on that pair sees every message published to it.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace feed_handler::messaging {

// Negative return codes of IPublication::offer (same values as Aeron).
constexpr int64_t kOfferNotConnected      = -1;
constexpr int64_t kOfferBackPressured     = -2;
constexpr int64_t kOfferAdminAction       = -3;
constexpr int64_t kOfferClosed            = -4;
constexpr int64_t kOfferMaxPositionExceeded = -5;

class IPublication {
public:
    virtual ~IPublication() = default;
    // >= 0: accepted. < 0: not sent (see codes above); the caller decides whether to retry or drop.
    virtual int64_t offer(const uint8_t *data, size_t len) = 0;
    virtual bool    is_connected() const = 0;     // at least one subscriber is attached
};

// Non-owning callback, so polling never allocates.
struct FragmentHandler {
    void *ctx = nullptr;
    void (*fn)(void *ctx, const uint8_t *data, size_t len) = nullptr;
    void operator()(const uint8_t *data, size_t len) const { fn(ctx, data, len); }
};

class ISubscription {
public:
    virtual ~ISubscription() = default;
    // Delivers up to `fragment_limit` messages; returns how many were delivered.
    virtual int  poll(FragmentHandler handler, int fragment_limit) = 0;
    virtual bool is_connected() const = 0;        // at least one publisher is attached
};

using PendingId = uint64_t;

class ITransport {
public:
    virtual ~ITransport() = default;

    enum class PollResult { Pending, Ready, Failed };

    // Publications are created asynchronously so a server's feed thread never blocks on
    // them. begin_publication returns 0 on immediate failure; poll with the returned id.
    virtual PendingId   begin_publication(const std::string &channel, int32_t stream_id) = 0;
    virtual PollResult  poll_publication(PendingId id, std::unique_ptr<IPublication> &out) = 0;

    // Blocking (used at start-up): returns null on timeout.
    virtual std::unique_ptr<ISubscription> add_subscription(const std::string &channel, int32_t stream_id,
                                                            std::chrono::milliseconds timeout) = 0;
};

// Blocking convenience for start-up paths (client connect).
std::unique_ptr<IPublication> add_publication_blocking(ITransport &t, const std::string &channel,
                                                       int32_t stream_id, std::chrono::milliseconds timeout);

} // namespace feed_handler::messaging

#pragma once
// Aeron implementation of ITransport, plus an embeddable media driver for development.
// Compiled only when ITCH_WITH_AERON is defined (CMake option of the same name).
//
// PRODUCTION: run the standalone `aeronmd` (built with this project) on its own pinned,
// isolated cores, one per host, and point every process at its directory (`aeron_dir`). The
// embedded driver exists for tests, demos, and single-process deployments.

#ifdef ITCH_WITH_AERON

#include <chrono>
#include <memory>
#include <string>

#include "itch/messaging/transport.hpp"

namespace aeron { class Aeron; }

namespace feed_handler::messaging {

class AeronTransport final : public ITransport {
public:
    // Connects to a running media driver (blocks up to `timeout`). Returns null and sets
    // `error` on failure. An empty aeron_dir means Aeron's default directory.
    static std::unique_ptr<AeronTransport> connect(const std::string &aeron_dir, std::string &error,
                                                   std::chrono::milliseconds timeout = std::chrono::milliseconds(10000));
    ~AeronTransport() override;

    PendingId  begin_publication(const std::string &channel, int32_t stream_id) override;
    PollResult poll_publication(PendingId id, std::unique_ptr<IPublication> &out) override;
    std::unique_ptr<ISubscription> add_subscription(const std::string &channel, int32_t stream_id,
                                                    std::chrono::milliseconds timeout) override;

private:
    explicit AeronTransport(std::shared_ptr<aeron::Aeron> aeron);
    std::shared_ptr<aeron::Aeron> aeron_;
};

// An in-process Aeron media driver (the C driver). Not for production hot paths: its threads
// are not pinned. Destroying it shuts the driver down; transports must be destroyed first.
class EmbeddedMediaDriver {
public:
    struct Options {
        std::string dir;                  // aeron directory; empty = a fresh unique directory under /tmp
        bool dedicated_threads = false;   // false: one shared thread; true: conductor/sender/receiver threads
    };
    static std::unique_ptr<EmbeddedMediaDriver> start(const Options &opts, std::string &error);
    ~EmbeddedMediaDriver();
    const std::string &dir() const { return dir_; }

private:
    EmbeddedMediaDriver() = default;
    std::string dir_;
    void *context_ = nullptr;   // aeron_driver_context_t*
    void *driver_  = nullptr;   // aeron_driver_t*
};

} // namespace feed_handler::messaging

#endif // ITCH_WITH_AERON

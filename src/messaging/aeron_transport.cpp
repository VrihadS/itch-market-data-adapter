#include "itch/messaging/aeron_transport.hpp"

#ifdef ITCH_WITH_AERON

#include <atomic>
#include <cstdio>
#include <thread>
#include <unistd.h>

extern "C" {
#include "aeronmd.h"
}
#include "Aeron.h"

namespace feed_handler::messaging
{

namespace
{

class AeronPublication final : public IPublication
{
  public:
    explicit AeronPublication(std::shared_ptr<aeron::ExclusivePublication> p) : pub_(std::move(p)) {}
    int64_t offer(const uint8_t *data, size_t len) override
    {
        aeron::concurrent::AtomicBuffer buf(const_cast<uint8_t *>(data), static_cast<aeron::util::index_t>(len));
        return pub_->offer(buf, 0, static_cast<aeron::util::index_t>(len));
    }
    bool is_connected() const override { return pub_->isConnected(); }

  private:
    std::shared_ptr<aeron::ExclusivePublication> pub_;
};

class AeronSubscription final : public ISubscription
{
  public:
    explicit AeronSubscription(std::shared_ptr<aeron::Subscription> s) : sub_(std::move(s)) {}
    int poll(FragmentHandler h, int fragment_limit) override
    {
        // The lambda captures only a pointer to the stack handler, so polling does not allocate.
        const FragmentHandler *hp = &h;
        return sub_->poll(
            [hp](aeron::concurrent::AtomicBuffer &buf, aeron::util::index_t offset, aeron::util::index_t length,
                 aeron::Header &) { (*hp)(buf.buffer() + offset, static_cast<size_t>(length)); },
            fragment_limit);
    }
    bool is_connected() const override { return sub_->isConnected(); }

  private:
    std::shared_ptr<aeron::Subscription> sub_;
};

} // namespace

std::unique_ptr<AeronTransport> AeronTransport::connect(const std::string &aeron_dir, std::string &error,
                                                        std::chrono::milliseconds timeout)
{
    try
    {
        aeron::Context ctx;
        if (!aeron_dir.empty()) ctx.aeronDir(aeron_dir);
        ctx.mediaDriverTimeout(static_cast<long>(timeout.count()));
        ctx.errorHandler([](const std::exception &e) { std::fprintf(stderr, "aeron error: %s\n", e.what()); });
        return std::unique_ptr<AeronTransport>(new AeronTransport(aeron::Aeron::connect(ctx)));
    }
    catch (const std::exception &e)
    {
        error = std::string("cannot connect to Aeron media driver") +
                (aeron_dir.empty() ? "" : " at " + aeron_dir) + ": " + e.what();
        return nullptr;
    }
}

AeronTransport::AeronTransport(std::shared_ptr<aeron::Aeron> a) : aeron_(std::move(a)) {}
AeronTransport::~AeronTransport() = default;

PendingId AeronTransport::begin_publication(const std::string &channel, int32_t stream_id)
{
    try { return static_cast<PendingId>(aeron_->addExclusivePublication(channel, stream_id)) + 1; }   // +1: 0 means failure
    catch (const std::exception &e) { std::fprintf(stderr, "aeron: addExclusivePublication(%s): %s\n", channel.c_str(), e.what()); return 0; }
}

ITransport::PollResult AeronTransport::poll_publication(PendingId id, std::unique_ptr<IPublication> &out)
{
    try
    {
        auto p = aeron_->findExclusivePublication(static_cast<int64_t>(id - 1));
        if (!p) return PollResult::Pending;     // a failed registration surfaces via the error handler; callers time out
        out = std::make_unique<AeronPublication>(std::move(p));
        return PollResult::Ready;
    }
    catch (const std::exception &) { return PollResult::Failed; }
}

std::unique_ptr<ISubscription> AeronTransport::add_subscription(const std::string &channel, int32_t stream_id,
                                                                std::chrono::milliseconds timeout)
{
    try
    {
        const int64_t id = aeron_->addSubscription(channel, stream_id);
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;)
        {
            if (auto s = aeron_->findSubscription(id)) return std::make_unique<AeronSubscription>(std::move(s));
            if (std::chrono::steady_clock::now() >= deadline) return nullptr;
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }
    }
    catch (const std::exception &e)
    {
        std::fprintf(stderr, "aeron: addSubscription(%s): %s\n", channel.c_str(), e.what());
        return nullptr;
    }
}

// ── embedded driver ─────────────────────────────────────────────────

std::unique_ptr<EmbeddedMediaDriver> EmbeddedMediaDriver::start(const Options &opts, std::string &error)
{
    static std::atomic<unsigned> counter{0};
    std::unique_ptr<EmbeddedMediaDriver> d(new EmbeddedMediaDriver());
    d->dir_ = opts.dir.empty()
                  ? "/tmp/aeron-itch-" + std::to_string(::getpid()) + "-" + std::to_string(counter++)
                  : opts.dir;

    aeron_driver_context_t *ctx = nullptr;
    aeron_driver_t *drv = nullptr;
    if (aeron_driver_context_init(&ctx) < 0) { error = std::string("driver context: ") + aeron_errmsg(); return nullptr; }
    aeron_driver_context_set_dir(ctx, d->dir_.c_str());
    aeron_driver_context_set_dir_delete_on_start(ctx, true);
    aeron_driver_context_set_dir_delete_on_shutdown(ctx, true);
    aeron_driver_context_set_threading_mode(ctx, opts.dedicated_threads ? AERON_THREADING_MODE_DEDICATED : AERON_THREADING_MODE_SHARED);
    if (aeron_driver_init(&drv, ctx) < 0 || aeron_driver_start(drv, false) < 0)
    {
        error = std::string("driver start: ") + aeron_errmsg();
        if (drv) aeron_driver_close(drv);
        aeron_driver_context_close(ctx);
        return nullptr;
    }
    d->context_ = ctx;
    d->driver_ = drv;
    return d;
}

EmbeddedMediaDriver::~EmbeddedMediaDriver()
{
    if (driver_) aeron_driver_close(static_cast<aeron_driver_t *>(driver_));
    if (context_) aeron_driver_context_close(static_cast<aeron_driver_context_t *>(context_));
}

} // namespace feed_handler::messaging

#endif // ITCH_WITH_AERON

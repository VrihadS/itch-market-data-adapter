#include "itch/messaging/transport.hpp"

#include <thread>

namespace feed_handler::messaging {

std::unique_ptr<IPublication> add_publication_blocking(ITransport &t, const std::string &channel,
                                                       int32_t stream_id, std::chrono::milliseconds timeout)
{
    const PendingId id = t.begin_publication(channel, stream_id);
    if (!id) return nullptr;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::unique_ptr<IPublication> pub;
    for (;;)
    {
        switch (t.poll_publication(id, pub))
        {
        case ITransport::PollResult::Ready:  return pub;
        case ITransport::PollResult::Failed: return nullptr;
        case ITransport::PollResult::Pending: break;
        }
        if (std::chrono::steady_clock::now() >= deadline) return nullptr;
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
}

} // namespace feed_handler::messaging

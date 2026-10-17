#include "itch/feed/feed_consumer.hpp"

#include <cstring>

namespace feed_handler::feed
{

int FeedConsumer::poll(int max_records)
{
    SpscRing::Record r;
    int n = 0;
    while (n < max_records && ring_.peek(r))
    {
        switch (r.type)
        {
        case kRecMessage:
        {
            uint64_t seq;
            std::memcpy(&seq, r.data, 8);
            size_t used = 0;
            if (!decoder_.DecodeMessage(reinterpret_cast<const char *>(r.data + 8), r.size - 8, used, books_))
                decode_failures_.store(decode_failures_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
            messages_.store(messages_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
            last_seq_.store(seq, std::memory_order_relaxed);
            break;
        }
        case kRecSessionStart:
        {
            const bool catching_up = r.data[10] != 0, rebuild = r.data[11] != 0;
            books_.reset();
            if (catching_up && !detached_)
            {
                detached_listener_ = books_.listener();       // hold back publishing while history replays
                books_.set_listener(nullptr);
                detached_ = true;
            }
            else if (!catching_up && detached_)
            {
                books_.set_listener(detached_listener_);
                detached_ = false;
            }
            session_starts_.store(session_starts_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
            set_mode(catching_up ? Mode::CatchingUp : Mode::Live);
            if (hooks_.on_session_start) hooks_.on_session_start(catching_up, rebuild);
            if (!catching_up && hooks_.on_live) hooks_.on_live();
            break;
        }
        case kRecCaughtUp:
            if (detached_) { books_.set_listener(detached_listener_); detached_ = false; }
            set_mode(Mode::Live);
            if (hooks_.on_live) hooks_.on_live();
            break;
        case kRecEndOfSession:
            set_mode(Mode::Ended);
            if (hooks_.on_end_of_session) hooks_.on_end_of_session();
            break;
        case kRecStale:
            set_mode(Mode::Stale);
            if (hooks_.on_stale) hooks_.on_stale();
            break;
        default:
            break;
        }
        ring_.pop();
        ++n;
    }
    return n;
}

}  // namespace feed_handler::feed

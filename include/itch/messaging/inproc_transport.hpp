#pragma once
// In-process transport for tests: no media driver, no threads, fully deterministic.
// Messages published to (channel, stream) are copied into the queue of every subscription
// on that pair. A publication with no subscriber reports not-connected, like Aeron.

#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "itch/messaging/transport.hpp"

namespace feed_handler::messaging {

class InprocTransport : public ITransport {
public:
    using Key = std::pair<std::string, int32_t>;

    struct SubQueue {
        std::deque<std::vector<uint8_t>> q;
        size_t capacity = 1u << 20;     // messages; offers beyond this are back-pressured
    };
    struct Stream {
        std::vector<std::shared_ptr<SubQueue>> subs;
    };

    // Test hooks.
    void   fail_next_publication(bool v = true) { fail_next_pub_ = v; }
    void   set_capacity(size_t messages) { default_capacity_ = messages; }
    size_t publications_created() const { return pubs_created_; }

    // Like Aeron, publications and subscriptions meet on the endpoint and stream; parameters
    // after '|' (interface, ttl, ...) only affect how the packets travel, not who matches.
    static std::string normalize(const std::string &channel) { return channel.substr(0, channel.find('|')); }

    PendingId begin_publication(const std::string &channel, int32_t stream_id) override {
        if (fail_next_pub_) { fail_next_pub_ = false; return 0; }
        pending_[++next_id_] = Key{normalize(channel), stream_id};
        return next_id_;
    }

    PollResult poll_publication(PendingId id, std::unique_ptr<IPublication> &out) override {
        auto it = pending_.find(id);
        if (it == pending_.end()) return PollResult::Failed;
        out = std::make_unique<Pub>(stream(it->second));
        pending_.erase(it);
        ++pubs_created_;
        return PollResult::Ready;
    }

    std::unique_ptr<ISubscription> add_subscription(const std::string &channel, int32_t stream_id,
                                                    std::chrono::milliseconds) override {
        auto sq = std::make_shared<SubQueue>();
        sq->capacity = default_capacity_;
        auto st = stream({normalize(channel), stream_id});
        st->subs.push_back(sq);
        return std::make_unique<Sub>(st, sq);
    }

private:
    std::shared_ptr<Stream> stream(const Key &k) {
        auto &s = streams_[k];
        if (!s) s = std::make_shared<Stream>();
        return s;
    }

    class Pub : public IPublication {
    public:
        explicit Pub(std::shared_ptr<Stream> s) : s_(std::move(s)) {}
        int64_t offer(const uint8_t *data, size_t len) override {
            prune();
            if (s_->subs.empty()) return kOfferNotConnected;
            for (auto &sq : s_->subs)
                if (sq->q.size() >= sq->capacity) return kOfferBackPressured;
            for (auto &sq : s_->subs) sq->q.emplace_back(data, data + len);
            return static_cast<int64_t>(len);
        }
        bool is_connected() const override { const_cast<Pub *>(this)->prune(); return !s_->subs.empty(); }
    private:
        void prune() {   // drop subscriptions that no longer exist (use_count == 1: only the stream holds it)
            auto &v = s_->subs;
            for (size_t i = 0; i < v.size();) { if (v[i].use_count() == 1) v.erase(v.begin() + i); else ++i; }
        }
        std::shared_ptr<Stream> s_;
    };

    class Sub : public ISubscription {
    public:
        Sub(std::shared_ptr<Stream> s, std::shared_ptr<SubQueue> q) : s_(std::move(s)), q_(std::move(q)) {}
        int poll(FragmentHandler h, int limit) override {
            int n = 0;
            while (n < limit && !q_->q.empty()) {
                std::vector<uint8_t> m = std::move(q_->q.front());
                q_->q.pop_front();
                h(m.data(), m.size());
                ++n;
            }
            return n;
        }
        bool is_connected() const override { return true; }
    private:
        std::shared_ptr<Stream>   s_;
        std::shared_ptr<SubQueue> q_;
    };

    std::map<Key, std::shared_ptr<Stream>> streams_;
    std::map<PendingId, Key>               pending_;
    PendingId                              next_id_ = 0;
    bool                                   fail_next_pub_ = false;
    size_t                                 default_capacity_ = 1u << 20;
    size_t                                 pubs_created_ = 0;
};

} // namespace feed_handler::messaging

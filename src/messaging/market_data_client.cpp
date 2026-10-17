#include "itch/messaging/market_data_client.hpp"

#include <chrono>
#include <random>
#include <thread>

#include "itch/messaging/channel.hpp"

namespace feed_handler::messaging
{

namespace
{
uint64_t steady_now_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
uint64_t realtime_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
} // namespace

MarketDataClient::MarketDataClient(ITransport &transport, ClientConfig cfg) : transport_(transport), cfg_(std::move(cfg))
{
    std::random_device rd;
    std::mt19937_64 g(rd());
    if (cfg_.client_id == 0) cfg_.client_id = g() | 1;
    if (cfg_.reply_stream_id == 0) cfg_.reply_stream_id = static_cast<int32_t>(10000 + g() % 2000000000u);
    if (!cfg_.now_ns) cfg_.now_ns = steady_now_ns;
}

bool MarketDataClient::connect()
{
    const bool ipc = cfg_.server_host == "ipc";
    std::string control_channel, local_reply_channel;
    if (ipc)
    {
        control_channel = local_reply_channel = request_channel_ = "aeron:ipc";
    }
    else
    {
        if (!parse_ipv4(cfg_.server_host)) { error_ = "server_host must be 'ipc' or an IPv4 address"; return false; }
        control_channel = make_udp_channel(cfg_.server_host, cfg_.control_port);
        if (!cfg_.multicast_group.empty())
        {
            const auto g = parse_ipv4(cfg_.multicast_group);
            if (!g || (*g >> 24) < 224 || (*g >> 24) > 239) { error_ = "multicast_group must be an IPv4 multicast address"; return false; }
            request_channel_ = make_udp_channel(cfg_.multicast_group, cfg_.reply_port);
            local_reply_channel = request_channel_;
            if (!cfg_.reply_host.empty()) local_reply_channel += "|interface=" + cfg_.reply_host;
        }
        else
        {
            if (!parse_ipv4(cfg_.reply_host)) { error_ = "reply_host (this machine's IPv4) is required for UDP"; return false; }
            request_channel_ = local_reply_channel = make_udp_channel(cfg_.reply_host, cfg_.reply_port);
        }
    }

    reply_ = transport_.add_subscription(local_reply_channel, cfg_.reply_stream_id, cfg_.connect_timeout);
    if (!reply_) { error_ = "could not open reply subscription on " + local_reply_channel; return false; }
    control_ = add_publication_blocking(transport_, control_channel, cfg_.control_stream_id, cfg_.connect_timeout);
    if (!control_) { error_ = "could not open control publication on " + control_channel; return false; }

    const auto deadline = std::chrono::steady_clock::now() + cfg_.connect_timeout;
    while (!control_->is_connected())
    {
        if (std::chrono::steady_clock::now() >= deadline) { error_ = "no server listening on " + control_channel; return false; }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    last_heard_ns_ = last_heartbeat_ns_ = now();
    connected_ = true;
    return true;
}

bool MarketDataClient::send(const uint8_t *data, size_t len)
{
    if (!control_) return false;
    for (int attempt = 0; attempt < 100; ++attempt)    // bounded retry on back-pressure
    {
        const int64_t rc = control_->offer(data, len);
        if (rc >= 0) return true;
        if (rc != kOfferBackPressured && rc != kOfferAdminAction) break;
        std::this_thread::yield();
    }
    ++stats_.requests_dropped;
    return false;
}

uint32_t MarketDataClient::subscribe(const std::vector<std::string> &symbols, uint8_t features, uint8_t depth)
{
    if (!connected_ || symbols.empty() || symbols.size() > kMaxSymbolsPerRequest) return 0;
    SubscribeRequest r{};
    r.client_id = cfg_.client_id;
    r.request_id = next_request_++;
    r.reply_stream_id = static_cast<uint32_t>(cfg_.reply_stream_id);
    r.features = features;
    r.depth = depth;
    r.symbol_count = static_cast<uint8_t>(symbols.size());
    if (!set_channel(r.reply_channel, request_channel_)) return 0;
    for (size_t i = 0; i < symbols.size(); ++i)
        if (!set_symbol(r.symbols[i], symbols[i])) return 0;
    uint8_t buf[kMaxMessageSize];
    if (!send(buf, encode(r, buf))) return 0;
    if (pending_.size() > 1024) pending_.clear();        // unanswered requests must not accumulate
    pending_[r.request_id] = PendingRequest{symbols, features, false, false};
    return r.request_id;
}

uint32_t MarketDataClient::unsubscribe(const std::vector<std::string> &symbols, uint8_t features)
{
    if (!connected_ || symbols.empty() || symbols.size() > kMaxSymbolsPerRequest) return 0;
    UnsubscribeRequest r{};
    r.client_id = cfg_.client_id;
    r.request_id = next_request_++;
    r.features = features;
    r.symbol_count = static_cast<uint8_t>(symbols.size());
    for (size_t i = 0; i < symbols.size(); ++i)
        if (!set_symbol(r.symbols[i], symbols[i])) return 0;
    uint8_t buf[kMaxMessageSize];
    if (!send(buf, encode(r, buf))) return 0;
    if (pending_.size() > 1024) pending_.clear();
    pending_[r.request_id] = PendingRequest{symbols, features, true, false};
    return r.request_id;
}

uint32_t MarketDataClient::disconnect()
{
    if (!connected_) return 0;
    UnsubscribeRequest r{};
    r.client_id = cfg_.client_id;
    r.request_id = next_request_++;
    r.symbol_count = 0;
    uint8_t buf[kMaxMessageSize];
    if (!send(buf, encode(r, buf))) return 0;
    pending_[r.request_id] = PendingRequest{{}, 0, true, true};
    return r.request_id;
}

void MarketDataClient::apply_ack(const SubscribeAck &a)
{
    const auto it = pending_.find(a.request_id);
    if (it == pending_.end()) return;
    const PendingRequest req = std::move(it->second);
    pending_.erase(it);
    if (static_cast<Status>(a.status) != Status::Ok) return;
    if (req.disconnect) { subscribed_.clear(); expected_.clear(); return; }
    for (uint8_t i = 0; i < a.result_count && i < req.symbols.size(); ++i)
    {
        const SymbolResult &res = a.results[i];
        if (res.locate == 0) continue;
        const uint32_t base = static_cast<uint32_t>(res.locate) << 1;
        if (!req.unsubscribe)
        {
            if (res.status != static_cast<uint8_t>(Status::Ok)) continue;
            subscribed_[res.locate] = req.features;                   // a new request replaces the old one
            expected_.erase(base | static_cast<uint32_t>(StreamKind::Trades));   // fresh sequence tracking
        }
        else if (res.status == static_cast<uint8_t>(Status::Ok))
        {
            auto sit = subscribed_.find(res.locate);
            if (sit == subscribed_.end()) continue;
            sit->second &= static_cast<uint8_t>(~req.features);
            if (sit->second == 0) { subscribed_.erase(sit); expected_.erase(base); expected_.erase(base | 1u); }
        }
    }
}

int MarketDataClient::poll(IMarketDataHandler &handler, int fragment_limit)
{
    if (!connected_) return 0;
    PollCtx ctx{this, &handler};
    const int n = reply_->poll(FragmentHandler{&ctx, &MarketDataClient::on_fragment}, fragment_limit);

    const uint64_t t = now();
    if (t - last_heartbeat_ns_ >= cfg_.heartbeat_interval_ns)
    {
        last_heartbeat_ns_ = t;
        Heartbeat hb{};
        hb.client_id = cfg_.client_id;
        hb.send_ts_ns = realtime_ns();
        uint8_t buf[kMaxMessageSize];
        send(buf, encode(hb, buf));
    }
    if (!timeout_reported_ && t - last_heard_ns_ > cfg_.server_timeout_ns)
    {
        timeout_reported_ = true;
        handler.on_server_timeout();
    }
    return n;
}

void MarketDataClient::on_fragment(void *ctx, const uint8_t *data, size_t len)
{
    auto *c = static_cast<PollCtx *>(ctx);
    c->self->dispatch(*c->handler, data, len);
}

void MarketDataClient::check_gap(IMarketDataHandler &h, uint16_t locate, StreamKind kind, uint64_t seq, bool resets)
{
    const uint32_t key = (static_cast<uint32_t>(locate) << 1) | static_cast<uint32_t>(kind);
    auto it = expected_.find(key);
    if (!resets && it != expected_.end() && seq != it->second)
    {
        ++stats_.gaps;
        h.on_gap(locate, kind, it->second, seq);
    }
    expected_[key] = seq + 1;
}

void MarketDataClient::dispatch(IMarketDataHandler &h, const uint8_t *data, size_t len)
{
    last_heard_ns_ = now();
    timeout_reported_ = false;
    MsgType t;
    if (!peek_header(data, len, t)) { ++stats_.malformed; return; }
    switch (t)
    {
    case MsgType::SubscribeAck:
    {
        SubscribeAck a;
        if (!decode(data, len, a)) { ++stats_.malformed; break; }
        if (a.client_id != cfg_.client_id) break;      // addressed to another client sharing this stream
        ++stats_.acks;
        apply_ack(a);
        AckView v{};
        v.request_id = a.request_id;
        v.status = static_cast<Status>(a.status);
        v.result_count = a.result_count;
        for (uint8_t i = 0; i < a.result_count; ++i) v.results[i] = a.results[i];
        h.on_ack(v);
        break;
    }
    case MsgType::L2Book:
    {
        L2View v{};
        L2Book head;
        if (!decode_l2(data, len, head, v.bids, v.asks)) { ++stats_.malformed; break; }
        v.locate = head.locate;
        v.snapshot = head.snapshot != 0;
        v.sequence = head.sequence;
        v.exchange_ts_ns = head.exchange_ts_ns;
        v.send_ts_ns = head.send_ts_ns;
        v.bid_count = head.bid_count;
        v.ask_count = head.ask_count;
        { const auto sit = subscribed_.find(v.locate);
          if (sit == subscribed_.end() || !(sit->second & kFeatureL2Book)) { ++stats_.filtered; break; } }
        ++stats_.l2;
        if (v.snapshot) ++stats_.snapshots;
        check_gap(h, v.locate, StreamKind::L2, v.sequence, v.snapshot);   // a snapshot re-bases the sequence
        h.on_l2(v);
        break;
    }
    case MsgType::Trade:
    {
        Trade m;
        if (!decode(data, len, m)) { ++stats_.malformed; break; }
        TradeView v{m.locate, m.kind, m.side, m.cross_type, m.sequence, m.exchange_ts_ns, m.send_ts_ns, m.price, m.shares, m.match_number};
        { const auto sit = subscribed_.find(v.locate);
          if (sit == subscribed_.end() || !(sit->second & kFeatureTimeAndSales)) { ++stats_.filtered; break; } }
        ++stats_.trades;
        check_gap(h, v.locate, StreamKind::Trades, v.sequence, /*resets=*/false);
        h.on_trade(v);
        break;
    }
    case MsgType::Heartbeat:
        ++stats_.heartbeats;
        break;
    default:
        ++stats_.malformed;
    }
}

} // namespace feed_handler::messaging

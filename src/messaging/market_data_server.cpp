#include "itch/messaging/market_data_server.hpp"

#include <algorithm>
#include <chrono>

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

MarketDataServer::MarketDataServer(ITransport &transport, BookManager &books, ServerConfig cfg)
    : transport_(transport), books_(books), cfg_(std::move(cfg)), routes_(BookManager::kMaxLocate)
{
    if (!cfg_.now_ns) cfg_.now_ns = steady_now_ns;
}

MarketDataServer::~MarketDataServer() { stop(); }

bool MarketDataServer::start()
{
    if (started_) return true;
    const auto timeout = std::chrono::milliseconds(5000);
    for (const std::string *ch : {&cfg_.control_ipc_channel, &cfg_.control_udp_channel})
    {
        if (ch->empty()) continue;
        auto sub = transport_.add_subscription(*ch, cfg_.control_stream_id, timeout);
        if (!sub) return false;
        control_subs_.push_back(std::move(sub));
    }
    if (control_subs_.empty()) return false;
    books_.set_listener(this);
    started_ = true;
    last_tick_ns_ = now();
    return true;
}

void MarketDataServer::stop()
{
    if (!started_) return;
    started_ = false;
    for (uint32_t loc = 0; loc < routes_.size(); ++loc)
        if (!routes_[loc].empty()) { routes_[loc].clear(); books_.set_interest(static_cast<uint16_t>(loc), 0, 0); }
    books_.set_listener(nullptr);
    clients_.clear();
    sinks_.clear();
    control_subs_.clear();
}

// ── control plane ──────────────────────────────────────────────────

int MarketDataServer::poll_control()
{
    if (!started_) return 0;
    int n = 0;
    FragmentHandler h{this, &MarketDataServer::on_control_fragment};
    for (auto &s : control_subs_) n += s->poll(h, 16);
    const uint64_t t = now();
    if (t - last_tick_ns_ >= cfg_.tick_interval_ns) { last_tick_ns_ = t; tick(t); }
    return n;
}

void MarketDataServer::on_control_fragment(void *ctx, const uint8_t *data, size_t len)
{
    static_cast<MarketDataServer *>(ctx)->handle_control(data, len);
}

void MarketDataServer::handle_control(const uint8_t *data, size_t len)
{
    MsgType t;
    if (!peek_header(data, len, t)) { ++stats_.malformed; return; }
    switch (t)
    {
    case MsgType::SubscribeRequest:
    {
        SubscribeRequest r;
        if (decode(data, len, r)) handle_subscribe(r); else ++stats_.malformed;
        break;
    }
    case MsgType::UnsubscribeRequest:
    {
        UnsubscribeRequest r;
        if (decode(data, len, r)) handle_unsubscribe(r); else ++stats_.malformed;
        break;
    }
    case MsgType::Heartbeat:
    {
        Heartbeat h;
        if (decode(data, len, h)) handle_heartbeat(h); else ++stats_.malformed;
        break;
    }
    default:
        ++stats_.malformed;   // data-plane messages do not belong on the control stream
    }
}

MarketDataServer::Sink *MarketDataServer::get_or_create_sink(const ParsedChannel &pc, std::string_view channel,
                                                             int32_t stream_id, uint64_t client_id, Status &status)
{
    std::string final_channel(channel);
    if (pc.kind == ChannelKind::UdpMulticast && !cfg_.multicast_interface.empty())
        final_channel += "|interface=" + cfg_.multicast_interface;
    const std::string key = final_channel + "#" + std::to_string(stream_id);

    auto it = sinks_.find(key);
    if (it != sinks_.end())
    {
        Sink *s = it->second.get();
        if (s->owner != 0 && s->owner != client_id) { status = Status::ChannelInUse; return nullptr; }
        return s;
    }
    auto s = std::make_unique<Sink>();
    s->key = key;
    s->channel = final_channel;
    s->stream_id = stream_id;
    s->kind = pc.kind;
    s->owner = (pc.kind == ChannelKind::UdpMulticast) ? 0 : client_id;
    s->created_ns = s->last_connected_ns = now();
    s->pending = transport_.begin_publication(final_channel, stream_id);
    if (!s->pending) { status = Status::BadChannel; return nullptr; }
    Sink *raw = s.get();
    sinks_[key] = std::move(s);
    return raw;
}

void MarketDataServer::release_sink(Sink *s)
{
    if (--s->refs <= 0) sinks_.erase(s->key);
}

void MarketDataServer::handle_subscribe(const SubscribeRequest &r)
{
    ++stats_.requests;
    const std::string_view channel = *get_channel(r.reply_channel);   // validated by decode()
    const ParsedChannel pc = parse_reply_channel(channel);
    bool allowed = pc.kind != ChannelKind::Invalid;
    if (allowed && pc.kind != ChannelKind::Ipc && !cfg_.allowed_reply_cidrs.empty())
    {
        allowed = std::any_of(cfg_.allowed_reply_cidrs.begin(), cfg_.allowed_reply_cidrs.end(),
                              [&](const Cidr &c) { return c.contains(pc.ip); });
    }
    // With no valid destination there is nobody to tell, so drop (and count) silently.
    if (!allowed) { ++stats_.dropped_unreplyable; ++stats_.rejected; return; }

    auto cit = clients_.find(r.client_id);
    if (cit == clients_.end() && clients_.size() >= cfg_.max_clients) { ++stats_.dropped_unreplyable; ++stats_.rejected; return; }

    Status top = Status::Ok;
    Sink *sink = nullptr;
    if (cit != clients_.end())
    {
        sink = cit->second.sink;
        // A client keeps one reply channel for its lifetime; to change it, disconnect first.
        std::string want = std::string(channel);
        if (pc.kind == ChannelKind::UdpMulticast && !cfg_.multicast_interface.empty())
            want += "|interface=" + cfg_.multicast_interface;
        if (sink->channel != want || sink->stream_id != static_cast<int32_t>(r.reply_stream_id)) top = Status::BadRequest;
    }
    else
    {
        sink = get_or_create_sink(pc, channel, static_cast<int32_t>(r.reply_stream_id), r.client_id, top);
        if (!sink) { ++stats_.rejected; ++stats_.dropped_unreplyable; return; }   // cannot reply on a channel we could not open
        Client c;
        c.id = r.client_id;
        c.sink = sink;
        c.last_seen_ns = now();
        ++sink->refs;
        cit = clients_.emplace(r.client_id, std::move(c)).first;
    }
    Client &client = cit->second;
    client.last_seen_ns = now();

    if (top == Status::Ok)
    {
        if (r.features == 0 || (r.features & ~kKnownFeatures)) top = Status::BadFeatures;
        else if ((r.features & kFeatureL2Book) && (r.depth < 1 || r.depth > kMaxL2Depth)) top = Status::BadDepth;
    }

    SubscribeAck ack{};
    ack.client_id = r.client_id;
    ack.request_id = r.request_id;
    ack.status = static_cast<uint8_t>(top);

    if (top == Status::Ok)
    {
        std::vector<uint16_t> changed;
        const uint8_t depth = (r.features & kFeatureL2Book) ? r.depth : 0;
        for (uint8_t i = 0; i < r.symbol_count; ++i)
        {
            SymbolResult &res = ack.results[ack.result_count++];
            res = {0, static_cast<uint8_t>(Status::Ok), 0};
            const auto loc = books_.locate_of(*get_symbol(r.symbols[i]));
            if (!loc) { res.status = static_cast<uint8_t>(Status::UnknownSymbol); continue; }
            if (client.subs.size() >= cfg_.max_subscriptions_per_client && !client.subs.count(*loc))
            {
                res.status = static_cast<uint8_t>(Status::TooManySubscriptions);
                continue;
            }
            res.locate = *loc;
            client.subs[*loc] = SubInfo{r.features, depth};
            changed.push_back(*loc);
            if (r.features & kFeatureL2Book) sink->snapshot_wanted.push_back(*loc);
        }
        for (uint16_t loc : changed) rebuild_routes(loc);
        ++stats_.accepted;
    }
    else
    {
        ++stats_.rejected;
    }

    uint8_t buf[kMaxMessageSize];
    send_or_queue(*sink, buf, encode(ack, buf));
}

void MarketDataServer::handle_unsubscribe(const UnsubscribeRequest &r)
{
    auto cit = clients_.find(r.client_id);
    if (cit == clients_.end()) return;
    Client &client = cit->second;
    client.last_seen_ns = now();
    Sink *sink = client.sink;

    SubscribeAck ack{};
    ack.client_id = r.client_id;
    ack.request_id = r.request_id;
    ack.status = static_cast<uint8_t>(Status::Ok);

    if (r.symbol_count == 0)    // disconnect: tell the client, then forget it
    {
        uint8_t buf[kMaxMessageSize];
        offer(*sink, buf, encode(ack, buf));
        remove_client(r.client_id);
        return;
    }
    std::vector<uint16_t> changed;
    for (uint8_t i = 0; i < r.symbol_count; ++i)
    {
        SymbolResult &res = ack.results[ack.result_count++];
        res = {0, static_cast<uint8_t>(Status::Ok), 0};
        const auto loc = books_.locate_of(*get_symbol(r.symbols[i]));
        if (!loc) { res.status = static_cast<uint8_t>(Status::UnknownSymbol); continue; }
        res.locate = *loc;
        auto sit = client.subs.find(*loc);
        if (sit == client.subs.end() || !(sit->second.features & r.features))
        {
            res.status = static_cast<uint8_t>(Status::NotSubscribed);
            continue;
        }
        sit->second.features &= static_cast<uint8_t>(~r.features);
        if (!(sit->second.features & kFeatureL2Book)) sit->second.depth = 0;
        if (sit->second.features == 0) client.subs.erase(sit);
        changed.push_back(*loc);
    }
    for (uint16_t loc : changed) rebuild_routes(loc);
    uint8_t buf[kMaxMessageSize];
    send_or_queue(*sink, buf, encode(ack, buf));
}

void MarketDataServer::handle_heartbeat(const Heartbeat &h)
{
    auto it = clients_.find(h.client_id);
    if (it != clients_.end()) it->second.last_seen_ns = now();
}

void MarketDataServer::remove_client(uint64_t id)
{
    auto it = clients_.find(id);
    if (it == clients_.end()) return;
    std::vector<uint16_t> locates;
    locates.reserve(it->second.subs.size());
    for (const auto &kv : it->second.subs) locates.push_back(kv.first);
    Sink *sink = it->second.sink;
    clients_.erase(it);
    for (uint16_t loc : locates) rebuild_routes(loc);
    release_sink(sink);
}

void MarketDataServer::rebuild_routes(uint16_t locate)
{
    std::vector<Route> fresh;
    for (auto &kv : clients_)
    {
        const Client &c = kv.second;
        const auto sit = c.subs.find(locate);
        if (sit == c.subs.end()) continue;
        auto rit = std::find_if(fresh.begin(), fresh.end(), [&](const Route &r) { return r.sink == c.sink; });
        if (rit == fresh.end())
        {
            Route r{c.sink, 0, 0, 0, 0};
            for (const Route &old : routes_[locate])        // keep sequence numbers across rebuilds
                if (old.sink == c.sink) { r.l2_seq = old.l2_seq; r.trade_seq = old.trade_seq; }
            fresh.push_back(r);
            rit = fresh.end() - 1;
        }
        rit->features |= sit->second.features;
        if (sit->second.features & kFeatureL2Book) rit->depth = std::max(rit->depth, sit->second.depth);
    }
    routes_[locate] = std::move(fresh);

    uint8_t f = 0, d = 0;
    for (const Route &r : routes_[locate]) { f |= r.features; d = std::max(d, r.depth); }
    books_.set_interest(locate, f, d);
}

// ── delivery on the control path ───────────────────────────────────

bool MarketDataServer::offer(Sink &s, const uint8_t *data, size_t len)
{
    if (!s.pub) return false;
    const int64_t rc = s.pub->offer(data, len);
    if (rc >= 0) return true;
    if (rc == kOfferNotConnected) ++stats_.dropped_not_connected; else ++stats_.dropped_back_pressure;
    return false;
}

void MarketDataServer::send_or_queue(Sink &s, const uint8_t *data, size_t len)
{
    if (s.active)
    {
        offer(s, data, len);
        flush_snapshots(s);
    }
    else
    {
        s.queued.emplace_back(data, data + len);
    }
}

void MarketDataServer::on_sink_active(Sink &s)
{
    for (const auto &m : s.queued) offer(s, m.data(), m.size());     // acks first, in order
    s.queued.clear();
    flush_snapshots(s);
}

void MarketDataServer::flush_snapshots(Sink &s)
{
    if (s.snapshot_wanted.empty()) return;
    std::vector<uint16_t> wanted;
    wanted.swap(s.snapshot_wanted);
    std::sort(wanted.begin(), wanted.end());
    wanted.erase(std::unique(wanted.begin(), wanted.end()), wanted.end());
    for (uint16_t loc : wanted)
        for (Route &r : routes_[loc])
            if (r.sink == &s && (r.features & kFeatureL2Book)) send_snapshot(r, loc);
}

void MarketDataServer::send_snapshot(Route &r, uint16_t locate)
{
    BookLevel bids[kMaxL2Depth], asks[kMaxL2Depth];
    L2Book head{};
    head.locate = locate;
    head.snapshot = 1;
    head.exchange_ts_ns = books_.last_timestamp_ns();
    head.send_ts_ns = realtime_ns();
    if (const OrderBook *b = books_.book(locate))
    {
        DepthLevel db[kMaxL2Depth], da[kMaxL2Depth];
        const size_t nb = b->depth(0, r.depth, db), na = b->depth(1, r.depth, da);
        for (size_t i = 0; i < nb; ++i) bids[i] = {db[i].price, db[i].orders, db[i].shares};
        for (size_t i = 0; i < na; ++i) asks[i] = {da[i].price, da[i].orders, da[i].shares};
        head.bid_count = static_cast<uint8_t>(nb);
        head.ask_count = static_cast<uint8_t>(na);
    }
    head.sequence = ++r.l2_seq;
    uint8_t buf[kMaxL2MessageSize];
    if (offer(*r.sink, buf, encode_l2(head, bids, asks, buf))) ++stats_.snapshots_sent;
}

void MarketDataServer::refresh_snapshots()
{
    if (!started_) return;
    for (uint32_t loc = 0; loc < routes_.size(); ++loc)
        for (Route &r : routes_[loc])
            if ((r.features & kFeatureL2Book) && r.sink->active) send_snapshot(r, static_cast<uint16_t>(loc));
}

// ── housekeeping ───────────────────────────────────────────────────

void MarketDataServer::tick(uint64_t t)
{
    std::vector<std::string> dead;          // keys: sinks may be erased while we clean up
    for (auto &kv : sinks_)
    {
        Sink &s = *kv.second;
        if (!s.pub)
        {
            switch (transport_.poll_publication(s.pending, s.pub))
            {
            case ITransport::PollResult::Ready:   break;
            case ITransport::PollResult::Failed:  dead.push_back(kv.first); continue;
            case ITransport::PollResult::Pending:
                if (t - s.created_ns > cfg_.connect_timeout_ns) dead.push_back(kv.first);
                continue;
            }
        }
        if (s.pub->is_connected())
        {
            s.ever_connected = true;
            s.last_connected_ns = t;
            if (!s.active) { s.active = true; on_sink_active(s); }
            if (t - s.last_heartbeat_ns >= cfg_.heartbeat_interval_ns)
            {
                s.last_heartbeat_ns = t;
                Heartbeat hb{};
                hb.client_id = s.owner;
                hb.send_ts_ns = realtime_ns();
                hb.from_server = 1;
                uint8_t buf[kMaxMessageSize];
                offer(s, buf, encode(hb, buf));
            }
        }
        else
        {
            s.active = false;
            const uint64_t limit = s.ever_connected ? cfg_.client_timeout_ns : cfg_.connect_timeout_ns;
            if (t - s.last_connected_ns > limit) dead.push_back(kv.first);
        }
    }
    for (const std::string &key : dead)    // sinks that never connected or went away: drop their clients
    {
        const auto sit = sinks_.find(key);
        if (sit == sinks_.end()) continue;
        const Sink *s = sit->second.get();
        std::vector<uint64_t> ids;
        for (auto &kv : clients_) if (kv.second.sink == s) ids.push_back(kv.first);
        for (uint64_t id : ids) { remove_client(id); ++stats_.clients_dropped; }
        sinks_.erase(key);                 // no-op if the last client's removal already released it
    }

    std::vector<uint64_t> expired;
    for (auto &kv : clients_)
        if (t - kv.second.last_seen_ns > cfg_.client_timeout_ns) expired.push_back(kv.first);
    for (uint64_t id : expired) { remove_client(id); ++stats_.clients_expired; }
}

// ── data plane (hot path) ──────────────────────────────────────────

void MarketDataServer::on_book_update(uint16_t locate, const OrderBook &book, uint64_t exchange_ts_ns)
{
    auto &routes = routes_[locate];
    uint8_t maxd = 0;
    for (const Route &r : routes)
        if ((r.features & kFeatureL2Book) && r.sink->active) maxd = std::max(maxd, r.depth);
    if (!maxd) return;

    DepthLevel db[kMaxL2Depth], da[kMaxL2Depth];
    const size_t nb = book.depth(0, maxd, db), na = book.depth(1, maxd, da);
    BookLevel bids[kMaxL2Depth], asks[kMaxL2Depth];
    for (size_t i = 0; i < nb; ++i) bids[i] = {db[i].price, db[i].orders, db[i].shares};
    for (size_t i = 0; i < na; ++i) asks[i] = {da[i].price, da[i].orders, da[i].shares};

    L2Book head{};
    head.locate = locate;
    head.exchange_ts_ns = exchange_ts_ns;
    head.send_ts_ns = realtime_ns();
    uint8_t buf[kMaxL2MessageSize];
    for (Route &r : routes)
    {
        if (!(r.features & kFeatureL2Book) || !r.sink->active) continue;
        head.bid_count = static_cast<uint8_t>(std::min<size_t>(nb, r.depth));
        head.ask_count = static_cast<uint8_t>(std::min<size_t>(na, r.depth));
        head.sequence = ++r.l2_seq;
        if (offer(*r.sink, buf, encode_l2(head, bids, asks, buf))) ++stats_.l2_sent;
    }
}

void MarketDataServer::on_trade(const TradeEvent &ev)
{
    auto &routes = routes_[ev.locate];
    if (routes.empty()) return;
    Trade m{};
    m.locate = ev.locate;
    m.kind = ev.kind;
    m.side = ev.side;
    m.cross_type = ev.cross_type;
    m.exchange_ts_ns = ev.exchange_ts_ns;
    m.send_ts_ns = realtime_ns();
    m.price = ev.price;
    m.shares = ev.shares;
    m.match_number = ev.match_number;
    uint8_t buf[kMaxMessageSize];
    for (Route &r : routes)
    {
        if (!(r.features & kFeatureTimeAndSales) || !r.sink->active) continue;
        m.sequence = ++r.trade_seq;
        if (offer(*r.sink, buf, encode(m, buf))) ++stats_.trades_sent;
    }
}

} // namespace feed_handler::messaging

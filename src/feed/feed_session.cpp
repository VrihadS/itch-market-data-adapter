#include "itch/feed/feed_session.hpp"

#include <algorithm>

namespace feed_handler::feed
{

const char *to_string(Line l)
{
    switch (l) { case Line::A: return "A"; case Line::B: return "B"; case Line::Rerequest: return "rerequest"; }
    return "?";
}

const char *to_string(State s)
{
    switch (s)
    {
    case State::WaitingForSession: return "waiting-for-session";
    case State::Live:              return "live";
    case State::CatchingUp:        return "catching-up";
    case State::Stale:             return "STALE";
    case State::Ended:             return "ended";
    }
    return "?";
}

FeedSession::FeedSession(FeedConfig cfg, IFeedSink &sink, IRerequestSender &rerequest)
    : cfg_(cfg), sink_(sink), rerequest_(rerequest)
{
    for (auto &u : line_up_) u.store(false, std::memory_order_relaxed);
    chunk_ = std::max<uint32_t>(1, std::min<uint32_t>(cfg_.max_request_messages, 64));
}

FeedStats FeedSession::stats() const
{
    FeedStats s;
    for (size_t i = 0; i < kLineCount; ++i)
    {
        const LineC &c = line_[i];
        s.line[i] = LineStats{c.packets.get(), c.bytes.get(), c.malformed.get(), c.heartbeats.get(), c.end_of_session.get(),
                              c.data_packets.get(), c.duplicate_packets.get(), c.messages_won.get(), c.outages.get(),
                              line_up_[i].load(std::memory_order_relaxed)};
    }
    s.state = state();
    s.next_sequence = next_pub_.load(std::memory_order_relaxed);
    s.highest_seen = highest_pub_.load(std::memory_order_relaxed);
    s.delivered = delivered_.get(); s.zero_length = zero_length_.get(); s.duplicate_messages = duplicate_messages_.get();
    s.gaps_detected = gaps_detected_.get(); s.gaps_closed = gaps_closed_.get();
    s.requests_sent = requests_sent_.get(); s.request_retries = request_retries_.get();
    s.request_send_failures = request_send_failures_.get(); s.recovered_messages = recovered_messages_.get();
    s.buffer_drops = buffer_drops_.get(); s.sink_blocked = sink_blocked_c_.get();
    s.session_starts = session_starts_.get(); s.sessions_ended = sessions_ended_.get();
    s.session_mismatch_packets = session_mismatch_.get(); s.stale_events = stale_events_.get();
    s.rebuilds = rebuilds_.get(); s.caught_up_events = caught_up_.get(); s.out_of_range_packets = out_of_range_.get();
    return s;
}

void FeedSession::note_high(uint64_t seq)
{
    if (seq > highest_) { highest_ = seq; highest_pub_.store(seq, std::memory_order_relaxed); }
}

// ── control events: delivered in order, retried if the sink is full ─────────────

bool FeedSession::try_emit(const Event &e)
{
    switch (e.kind)
    {
    case EventKind::SessionStart: return sink_.on_session_start(e.session, e.a, e.flag1, e.flag2);
    case EventKind::CaughtUp:     return sink_.on_caught_up(e.a);
    case EventKind::EndOfSession: return sink_.on_end_of_session(e.session, e.a);
    case EventKind::Stale:        return sink_.on_stale(StaleReason::GapUnrecoverable, e.a, e.b);
    }
    return true;
}

void FeedSession::emit(const Event &e)
{
    if (pending_events_.empty() && try_emit(e)) return;
    pending_events_.push_back(e);
}

bool FeedSession::flush_events()
{
    size_t done = 0;
    while (done < pending_events_.size() && try_emit(pending_events_[done])) ++done;
    pending_events_.erase(pending_events_.begin(), pending_events_.begin() + static_cast<std::ptrdiff_t>(done));
    return pending_events_.empty();
}

// ── sessions ───────────────────────────────────────────────────────────────────

void FeedSession::begin_session(const mold::Packet &p, uint64_t now, bool rebuild)
{
    session_ = p.header.session;
    have_session_ = true;
    buffer_.clear();
    buffered_bytes_ = 0;
    gap_active_ = false;
    win_.clear();
    eos_seen_ = false;
    eos_next_ = 0;
    highest_ = 0;
    highest_pub_.store(0, std::memory_order_relaxed);
    sink_blocked_ = false;

    const uint64_t first_seen = p.kind == mold::PacketKind::Data ? p.first_sequence() : p.header.sequence;
    next_ = cfg_.join_from_start ? 1 : first_seen;
    next_pub_.store(next_, std::memory_order_relaxed);
    const bool catching_up = first_seen > next_;
    set_state(catching_up ? State::CatchingUp : State::Live);
    last_session_rx_ns_ = now;
    session_starts_.add();
    Event e{EventKind::SessionStart, session_, next_, 0, catching_up, rebuild};
    emit(e);
}

bool FeedSession::check_session(const mold::Packet &p, uint64_t now)
{
    if (!have_session_) { begin_session(p, now, false); return true; }
    if (p.header.session == session_) { last_session_rx_ns_ = now; return true; }
    // A different session. The spec's receiver "aborts and reports the error" on a mismatch; here a
    // stray packet must not take down a healthy feed, so it is counted and ignored unless the current
    // session is over (end of session seen and fully delivered) or has gone silent.
    if (state() == State::Ended || now - last_session_rx_ns_ > cfg_.session_switch_timeout_ns)
    {
        begin_session(p, now, false);
        return true;
    }
    session_mismatch_.add();
    return false;
}

void FeedSession::rebuild(uint64_t now)
{
    if (!have_session_) return;
    rebuilds_.add();
    buffer_.clear();
    buffered_bytes_ = 0;
    gap_active_ = false;
    win_.clear();
    sink_blocked_ = false;
    next_ = 1;
    next_pub_.store(1, std::memory_order_relaxed);
    set_state(State::CatchingUp);
    last_session_rx_ns_ = now;
    emit(Event{EventKind::SessionStart, session_, 1, 0, true, true});
}

// ── packets ─────────────────────────────────────────────────────────────────────

void FeedSession::on_packet(Line line, const uint8_t *data, size_t len, uint64_t now)
{
    if (!started_) { started_ = true; start_ns_ = now; }
    LineC &lc = line_[static_cast<size_t>(line)];
    lc.packets.add();
    lc.bytes.add(len);

    mold::Packet p;
    if (mold::parse_packet(data, len, p) != mold::ParseError::None) { lc.malformed.add(); return; }
    // Sequence numbers start at 1; zero or absurdly large values mean a corrupt packet.
    if (p.header.sequence == 0 || p.end_sequence() > cfg_.max_sequence) { out_of_range_.add(); return; }

    lc.last_rx_ns = now;
    lc.ever_rx = true;
    if (!line_up_[static_cast<size_t>(line)].load(std::memory_order_relaxed))
        line_up_[static_cast<size_t>(line)].store(true, std::memory_order_relaxed);

    if (!check_session(p, now)) return;

    switch (p.kind)
    {
    case mold::PacketKind::Heartbeat:
        lc.heartbeats.add();
        note_high(p.header.sequence);
        break;
    case mold::PacketKind::EndOfSession:
        lc.end_of_session.add();
        eos_seen_ = true;
        eos_next_ = p.header.sequence;
        note_high(p.header.sequence);
        break;
    case mold::PacketKind::Data:
        lc.data_packets.add();
        handle_data(line, data, len, p, now);
        break;
    }
    advance(now);
}

void FeedSession::handle_data(Line line, const uint8_t *data, size_t len, const mold::Packet &p, uint64_t now)
{
    LineC &lc = line_[static_cast<size_t>(line)];
    const uint64_t first = p.first_sequence(), end = p.end_sequence();
    note_high(end);
    if (line == Line::Rerequest) note_reply(first, p.message_count(), now);
    const State st = state();
    if (st == State::Stale || st == State::Ended || st == State::WaitingForSession)
    {
        lc.duplicate_packets.add();   // nothing is delivered in these states; what was missed is re-requested after a rebuild
        return;
    }
    if (end <= next_)
    {
        lc.duplicate_packets.add();
        duplicate_messages_.add(p.message_count());
        return;
    }
    if (first <= next_ && !sink_blocked_ && pending_events_.empty() && (buffer_.empty() || buffer_.begin()->first > next_))
    {
        deliver_from(line, p, now);              // the common case: in order, straight from the datagram
        if (next_ < end) buffer_packet(line, data, len, p);   // sink filled up part-way: keep the rest
        return;
    }
    buffer_packet(line, data, len, p);           // ahead of us, or something older is still waiting
    drain_buffer(now);
}

// Delivers the messages of `p` that are at or after next_. Stops (and flags the sink as blocked) when
// the sink has no room. Returns the number of messages consumed.
uint64_t FeedSession::deliver_from(Line line, const mold::Packet &p, uint64_t now)
{
    (void)now;
    LineC &lc = line_[static_cast<size_t>(line)];
    uint64_t seq = p.first_sequence(), consumed = 0;
    mold::BlockIterator it(p);
    const uint8_t *msg;
    uint16_t len;
    while (it.next(msg, len))
    {
        if (seq < next_) { ++seq; continue; }             // already delivered (overlapping retransmission)
        if (len == 0) zero_length_.add();
        else if (!sink_.on_message(seq, msg, len))
        {
            if (!sink_blocked_) sink_blocked_c_.add();
            sink_blocked_ = true;
            next_pub_.store(next_, std::memory_order_relaxed);
            return consumed;
        }
        ++seq;
        ++consumed;
        next_ = seq;
        delivered_.add();
        lc.messages_won.add();
        if (line == Line::Rerequest) recovered_messages_.add();
    }
    next_pub_.store(next_, std::memory_order_relaxed);
    return consumed;
}

void FeedSession::buffer_packet(Line line, const uint8_t *data, size_t len, const mold::Packet &p)
{
    const uint64_t first = p.first_sequence();
    auto it = buffer_.find(first);
    if (it != buffer_.end())
    {
        if (it->second.end >= p.end_sequence()) return;           // already holding an equal or larger copy
        buffered_bytes_ -= it->second.bytes.size();
        buffer_.erase(it);
    }
    // Keep the packets nearest to what we need; if full, the farthest goes (it will be re-requested).
    while (!buffer_.empty() && (buffer_.size() >= cfg_.max_buffered_packets || buffered_bytes_ + len > cfg_.max_buffered_bytes))
    {
        auto far = std::prev(buffer_.end());
        if (far->first <= first) { buffer_drops_.add(); return; }  // the new one is the farthest: drop it
        buffered_bytes_ -= far->second.bytes.size();
        buffer_.erase(far);
        buffer_drops_.add();
    }
    buffered_bytes_ += len;
    buffer_[first] = Buffered{line, p.end_sequence(), std::vector<uint8_t>(data, data + len)};
}

void FeedSession::drain_buffer(uint64_t now)
{
    if (!pending_events_.empty() && !flush_events()) return;
    while (!buffer_.empty() && !sink_blocked_)
    {
        auto it = buffer_.begin();
        if (it->first > next_) break;                          // still a gap in front of it
        const uint64_t first = it->first;
        Buffered b = std::move(it->second);
        buffered_bytes_ -= b.bytes.size();
        buffer_.erase(it);
        if (b.end <= next_) { duplicate_messages_.add(b.end - first); continue; }
        mold::Packet p;
        if (mold::parse_packet(b.bytes.data(), b.bytes.size(), p) != mold::ParseError::None) continue;   // validated on arrival
        deliver_from(b.line, p, now);
        if (next_ < b.end)                                      // sink full again: keep the rest
        {
            buffered_bytes_ += b.bytes.size();
            buffer_[p.first_sequence()] = std::move(b);
            break;
        }
    }
}

// ── state machine: gaps, catching up, end of session ─────────────────────────────────

void FeedSession::advance(uint64_t now)
{
    const State st = state();
    if (st == State::WaitingForSession || st == State::Ended) return;
    if (!pending_events_.empty()) flush_events();
    if (st == State::Stale)
    {
        if (!cfg_.auto_rebuild_after_ns || now - stale_since_ns_ < cfg_.auto_rebuild_after_ns) return;
        rebuild(now);                      // and carry on: the replay starts in this same pass
    }
    sink_blocked_ = false;                 // retry delivery: the sink may have room now (this sets it again if not)
    if (pending_events_.empty()) drain_buffer(now);

    if (!gap_open())
    {
        if (gap_active_) { gap_active_ = false; win_.clear(); gaps_closed_.add(); }
        if (!sink_blocked_ && pending_events_.empty())
        {
            if (state() == State::CatchingUp)
            {
                set_state(State::Live);
                caught_up_.add();
                emit(Event{EventKind::CaughtUp, session_, next_, 0, false, false});
            }
            if (eos_seen_ && next_ >= eos_next_)
            {
                set_state(State::Ended);
                sessions_ended_.add();
                emit(Event{EventKind::EndOfSession, session_, next_, 0, false, false});
            }
        }
        return;
    }
    if (!gap_active_)
    {
        gap_active_ = true;
        gap_since_ns_ = now;
        stall_pos_ = next_;
        stall_since_ns_ = now;
        if (state() == State::Live) gaps_detected_.add();
    }
    // Progress at the blocking position resets the clock; no progress for too long means no recovery is coming.
    if (next_ != stall_pos_ || sink_blocked_ || !pending_events_.empty()) { stall_pos_ = next_; stall_since_ns_ = now; }
    else if (now - stall_since_ns_ > cfg_.gap_wait_ns + cfg_.request_timeout_ns * (cfg_.max_request_attempts + 1ull))
    {
        go_stale(now);
        return;
    }
    maybe_request(now);
}

// A reply from the re-request server tells us how much one reply holds; the request it answers is trimmed to
// that (the server will never send the rest of it) and the remainder gets requested again.
uint64_t FeedSession::timeout_for(const Req &r) const
{
    const uint64_t base = rto_ns_ ? rto_ns_ : cfg_.request_timeout_ns;
    const uint64_t backed_off = base << std::min<uint32_t>(r.attempts - 1, 6);
    return std::clamp(backed_off, cfg_.min_request_timeout_ns, std::max(cfg_.min_request_timeout_ns, cfg_.request_timeout_ns));
}

void FeedSession::note_reply(uint64_t first, uint64_t count, uint64_t now)
{
    for (Req &r : win_)
    {
        if (first < r.first || first >= r.first + r.count) continue;
        if (r.attempts == 1 && first == r.first && now >= r.sent_ns)       // only replies to a request sent once are unambiguous
        {
            const uint64_t rtt = now - r.sent_ns;
            if (!rto_ns_) { srtt_ns_ = rtt; rttvar_ns_ = rtt / 2; }
            else
            {
                const uint64_t err = srtt_ns_ > rtt ? srtt_ns_ - rtt : rtt - srtt_ns_;
                rttvar_ns_ = (3 * rttvar_ns_ + err) / 4;
                srtt_ns_ = (7 * srtt_ns_ + rtt) / 8;
            }
            rto_ns_ = std::clamp(srtt_ns_ + 4 * rttvar_ns_, cfg_.min_request_timeout_ns, std::max(cfg_.min_request_timeout_ns, cfg_.request_timeout_ns));
        }
        const uint64_t covered = first - r.first + count;
        if (covered < r.count)
        {
            r.count = covered;
            chunk_ = static_cast<uint32_t>(std::max<uint64_t>(1, count));                    // the server's capacity
        }
        else
            chunk_ = std::min<uint32_t>(cfg_.max_request_messages, chunk_ * 2);             // fits: probe for more
        return;
    }
}

// End of the already-handled or already-requested range containing `seq`, or `seq` itself if nothing covers it.
uint64_t FeedSession::covered_end(uint64_t seq) const
{
    uint64_t end = seq;
    auto it = buffer_.upper_bound(seq);
    if (it != buffer_.begin())
    {
        --it;
        if (it->second.end > seq) end = std::max(end, it->second.end);
    }
    for (const Req &r : win_)
        if (r.first <= seq && seq < r.first + r.count) end = std::max(end, r.first + r.count);
    return end;
}

void FeedSession::fill_window(uint64_t now)
{
    // Never keep more requests in flight than the reorder buffer could hold the replies to.
    const size_t room = cfg_.max_buffered_packets > buffer_.size() ? cfg_.max_buffered_packets - buffer_.size() : 0;
    const size_t window = std::max<size_t>(1, std::min<size_t>(cfg_.request_window, room));
    // This runs for every packet while a gap is open, so it must stay cheap: look at most kMaxScanSteps ranges
    // ahead (a long run of buffered packets would otherwise cost one map lookup per packet, per incoming packet).
    constexpr int kMaxScanSteps = 64;
    uint64_t cursor = next_;
    int steps = 0;
    while (cursor < highest_ && steps++ < kMaxScanSteps)
    {
        const uint64_t e = covered_end(cursor);
        if (e > cursor) { cursor = e; continue; }
        // The hole at next_ is what blocks delivery: it is ALWAYS requested, even if requests for data
        // further ahead already fill the window. Anything else waits for a free slot.
        if (win_.size() >= window && cursor != next_) break;
        // `cursor` starts a hole; it ends where the next buffered packet or request begins.
        uint64_t hole_end = highest_;
        auto b = buffer_.upper_bound(cursor);
        if (b != buffer_.end()) hole_end = std::min(hole_end, b->first);
        for (const Req &r : win_)
            if (r.first > cursor) hole_end = std::min(hole_end, r.first);
        const uint64_t count = std::min<uint64_t>({hole_end - cursor, chunk_, 65534});
        if (!rerequest_.send_request(session_, cursor, static_cast<uint16_t>(count)))
        {
            request_send_failures_.add();                 // try again on the next tick
            return;
        }
        requests_sent_.add();
        const Req r{cursor, count, now, 1};
        win_.insert(std::upper_bound(win_.begin(), win_.end(), r, [](const Req &x, const Req &y) { return x.first < y.first; }), r);
        cursor += count;
    }
}

void FeedSession::maybe_request(uint64_t now)
{
    // If the sink is full the "gap" is our own backlog, not loss: do not bother the server.
    if (sink_blocked_ || !pending_events_.empty()) { gap_since_ns_ = now; return; }
    // Give the other line a chance first (not while catching up: that gap is known to need the server).
    if (state() == State::Live && now - gap_since_ns_ < cfg_.gap_wait_ns) return;

    win_.erase(std::remove_if(win_.begin(), win_.end(), [&](const Req &r) { return r.first + r.count <= next_; }), win_.end());

    const bool buffer_full = buffer_.size() >= cfg_.max_buffered_packets;
    for (size_t i = 0; i < win_.size();)                  // requests that got no reply: send them again
    {
        Req &r = win_[i];
        if (now - r.sent_ns < timeout_for(r)) { ++i; continue; }
        // Only the request for the sequence we are waiting on can make the session give up: a reply for data
        // further ahead may simply have been discarded for lack of room, which says nothing about the server.
        const bool blocking = r.first <= next_;
        if (!blocking && buffer_full) { win_.erase(win_.begin() + static_cast<std::ptrdiff_t>(i)); continue; }   // ask again when there is room
        if (blocking && r.attempts >= cfg_.max_request_attempts) { go_stale(now); return; }
        if (r.first < next_) { r.count -= next_ - r.first; r.first = next_; }   // part of it arrived meanwhile
        if (!rerequest_.send_request(session_, r.first, static_cast<uint16_t>(r.count))) { request_send_failures_.add(); ++i; continue; }
        if (blocking) ++r.attempts;
        r.sent_ns = now;
        request_retries_.add();
        ++i;
    }
    fill_window(now);
}

void FeedSession::go_stale(uint64_t now)
{
    set_state(State::Stale);
    stale_since_ns_ = now;
    stale_events_.add();
    win_.clear();
    gap_active_ = false;
    buffer_.clear();
    buffered_bytes_ = 0;
    emit(Event{EventKind::Stale, session_, next_, highest_, false, false});
}

void FeedSession::tick(uint64_t now)
{
    if (!started_) return;
    for (size_t i = 0; i < 2; ++i)    // lines A and B; a line that never delivered anything is simply "not up"
    {
        LineC &lc = line_[i];
        if (line_up_[i].load(std::memory_order_relaxed) && now - lc.last_rx_ns > cfg_.line_timeout_ns)
        {
            line_up_[i].store(false, std::memory_order_relaxed);
            lc.outages.add();
        }
    }
    advance(now);
}

}  // namespace feed_handler::feed

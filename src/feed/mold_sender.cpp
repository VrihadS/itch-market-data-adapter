#include "itch/feed/mold_sender.hpp"

#include <algorithm>
#include <cstring>

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

namespace feed_handler::feed
{

// ── MessageLog ───────────────────────────────────────────────────────────────

uint64_t MessageLog::append(const uint8_t *data, size_t len)
{
    std::lock_guard<std::mutex> g(mu_);
    index_.emplace_back(arena_.size(), static_cast<uint32_t>(len));
    arena_.insert(arena_.end(), data, data + len);
    return base_ + index_.size() - 1;
}

uint64_t MessageLog::next_sequence() const { std::lock_guard<std::mutex> g(mu_); return base_ + index_.size(); }
uint64_t MessageLog::first_sequence() const { std::lock_guard<std::mutex> g(mu_); return base_; }

void MessageLog::trim_before(uint64_t seq)
{
    std::lock_guard<std::mutex> g(mu_);
    if (seq <= base_) return;
    const size_t drop = static_cast<size_t>(std::min<uint64_t>(seq - base_, index_.size()));
    index_.erase(index_.begin(), index_.begin() + static_cast<std::ptrdiff_t>(drop));
    base_ += drop;                      // the arena keeps its bytes; only the index forgets them
}

bool MessageLog::get(uint64_t seq, std::vector<uint8_t> &out) const
{
    std::lock_guard<std::mutex> g(mu_);
    if (seq < base_ || seq >= base_ + index_.size()) return false;
    const auto &e = index_[seq - base_];
    out.assign(arena_.begin() + static_cast<std::ptrdiff_t>(e.first), arena_.begin() + static_cast<std::ptrdiff_t>(e.first + e.second));
    return true;
}

std::vector<uint8_t> MessageLog::build_packet(const mold::Session &s, uint64_t first, uint32_t count, size_t max_packet_bytes,
                                              uint64_t limit) const
{
    std::lock_guard<std::mutex> g(mu_);
    std::vector<uint8_t> pkt;
    const uint64_t end_held = std::min<uint64_t>(base_ + index_.size(), limit);
    if (count == 0 || first < base_ || first >= end_held) return pkt;
    pkt.assign(mold::kHeaderSize, 0);
    uint16_t n = 0;
    for (uint64_t seq = first; n < count && n < 0xFFFE && seq < end_held; ++seq, ++n)
    {
        const auto &e = index_[seq - base_];
        if (pkt.size() + mold::kBlockOverhead + e.second > max_packet_bytes && n > 0) break;   // only what fits (at least one)
        uint8_t l[2];
        itch::write_be16(l, static_cast<uint16_t>(e.second));
        pkt.insert(pkt.end(), l, l + 2);
        pkt.insert(pkt.end(), arena_.begin() + static_cast<std::ptrdiff_t>(e.first), arena_.begin() + static_cast<std::ptrdiff_t>(e.first + e.second));
    }
    mold::write_header(pkt.data(), s, first, n);
    return pkt;
}

// ── Packetizer ───────────────────────────────────────────────────────────────

bool Packetizer::add(const uint8_t *msg, size_t len, std::vector<uint8_t> &out)
{
    if (builder_.add(msg, len)) return false;
    bool completed = false;
    if (!builder_.empty())
    {
        out.assign(buf_.data(), buf_.data() + builder_.finish());
        next_ = builder_.next_sequence();
        completed = true;
    }
    builder_.begin(session_, next_);
    builder_.add(msg, len);             // a message too large for any packet is dropped here; ITCH messages are < 64 bytes
    return completed;
}

bool Packetizer::flush(std::vector<uint8_t> &out)
{
    if (builder_.empty()) return false;
    out.assign(buf_.data(), buf_.data() + builder_.finish());
    next_ = builder_.next_sequence();
    builder_.begin(session_, next_);
    return true;
}

// ── FaultInjector ────────────────────────────────────────────────────────────

void FaultInjector::apply(const uint8_t *packet, size_t len, uint64_t send_ns, std::vector<Delivery> &out)
{
    if (burst_left_ > 0) { --burst_left_; ++dropped_; return; }
    if (cfg_.burst_len > 0 && u() < cfg_.burst_start) { burst_left_ = cfg_.burst_len - 1; ++dropped_; return; }
    if (u() < cfg_.loss) { ++dropped_; return; }

    auto make = [&](bool allow_corrupt) {
        Delivery d;
        d.bytes.assign(packet, packet + len);
        d.due_ns = send_ns + cfg_.base_delay_ns;
        if (cfg_.jitter_ns) d.due_ns += std::uniform_int_distribution<uint64_t>(0, cfg_.jitter_ns)(rng_);
        if (cfg_.reorder > 0 && u() < cfg_.reorder) d.due_ns += cfg_.reorder_delay_ns;
        if (allow_corrupt && cfg_.corrupt > 0 && u() < cfg_.corrupt)
        {
            ++corrupted_;
            switch (rng_() % 3)
            {
            case 0: d.bytes.resize(rng_() % d.bytes.size()); break;                               // truncated
            case 1: d.bytes.push_back(static_cast<uint8_t>(rng_())); break;                       // junk after the blocks
            default: d.bytes.insert(d.bytes.begin() + mold::kHeaderSize, 0xFF); break;            // shifts every block: framing broken
            }
        }
        return d;
    };
    out.push_back(make(true));
    if (u() < cfg_.duplicate) { ++duplicated_; out.push_back(make(false)); }
}

// ── UdpSender ────────────────────────────────────────────────────────────────

UdpSender::~UdpSender() { close(); }

void UdpSender::close()
{
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

bool UdpSender::open(const std::string &dest_ip, uint16_t port, const std::string &iface_ip, int ttl, std::string &error)
{
    close();
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    if (::inet_pton(AF_INET, dest_ip.c_str(), &a.sin_addr) != 1) { error = "bad destination address " + dest_ip; return false; }
    fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd_ < 0) { error = std::string("socket: ") + std::strerror(errno); return false; }
    const bool multicast = (ntohl(a.sin_addr.s_addr) >> 28) == 0xE;
    if (multicast)
    {
        if (!iface_ip.empty())
        {
            in_addr ifa{};
            if (::inet_pton(AF_INET, iface_ip.c_str(), &ifa) != 1) { error = "bad interface address " + iface_ip; return false; }
            ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_IF, &ifa, sizeof ifa);
        }
        unsigned char t = static_cast<unsigned char>(ttl), loop = 1;
        ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_TTL, &t, sizeof t);
        ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof loop);
    }
    addr_.assign(reinterpret_cast<uint8_t *>(&a), reinterpret_cast<uint8_t *>(&a) + sizeof a);
    return true;
}

bool UdpSender::send(const uint8_t *data, size_t len)
{
    if (fd_ < 0) return false;
    return ::sendto(fd_, data, len, 0, reinterpret_cast<const sockaddr *>(addr_.data()), static_cast<socklen_t>(addr_.size())) == static_cast<ssize_t>(len);
}

// ── UdpRerequestServer ─────────────────────────────────────────────────────

bool UdpRerequestServer::start(const std::string &bind_ip, uint16_t port, std::string &error)
{
    fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd_ < 0) { error = std::string("socket: ") + std::strerror(errno); return false; }
    const int on = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    if (::inet_pton(AF_INET, bind_ip.c_str(), &a.sin_addr) != 1) { error = "bad bind address"; return false; }
    if (::bind(fd_, reinterpret_cast<sockaddr *>(&a), sizeof a) < 0) { error = std::string("bind: ") + std::strerror(errno); ::close(fd_); fd_ = -1; return false; }
    socklen_t l = sizeof a;
    ::getsockname(fd_, reinterpret_cast<sockaddr *>(&a), &l);
    port_ = ntohs(a.sin_port);
    stop_ = false;
    thread_ = std::thread([this] { run(); });
    return true;
}

void UdpRerequestServer::stop()
{
    stop_ = true;
    if (thread_.joinable()) thread_.join();
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

void UdpRerequestServer::run()
{
    std::mt19937_64 rng(opt_.seed);
    std::uniform_real_distribution<double> uni(0.0, 1.0);
    uint8_t buf[2048];
    while (!stop_)
    {
        fd_set rf;
        FD_ZERO(&rf);
        FD_SET(fd_, &rf);
        timeval tv{0, 50'000};
        if (::select(fd_ + 1, &rf, nullptr, nullptr, &tv) <= 0) continue;
        sockaddr_in from{};
        socklen_t fl = sizeof from;
        const ssize_t n = ::recvfrom(fd_, buf, sizeof buf, 0, reinterpret_cast<sockaddr *>(&from), &fl);
        if (n != static_cast<ssize_t>(mold::kHeaderSize)) { ++ignored_; continue; }      // a request is exactly one header
        mold::Session s;
        std::memcpy(s.id, buf, mold::kSessionLen);
        const uint64_t first = itch::read_be64(buf + 10);
        const uint16_t count = itch::read_be16(buf + 18);
        ++received_;
        if (s != session_ || (opt_.drop_probability > 0 && uni(rng) < opt_.drop_probability)) { ++ignored_; continue; }
        const auto pkt = log_.build_packet(session_, first, count, opt_.max_packet_bytes, sent_next_.load());
        if (pkt.empty()) { ++ignored_; continue; }
        ::sendto(fd_, pkt.data(), pkt.size(), 0, reinterpret_cast<sockaddr *>(&from), fl);
        ++served_;
    }
}

// ── MoldFeedSender ──────────────────────────────────────────────────────────────────────────────────────

MoldFeedSender::MoldFeedSender(const Config &cfg, UdpSender &line_a, UdpSender &line_b, MessageLog &log, std::atomic<uint64_t> &sent_next)
    : cfg_(cfg), line_{&line_a, &line_b}, log_(log), sent_next_(sent_next), packetizer_(cfg.session, cfg.max_packet_bytes),
      fa_(cfg.fault_a, cfg.seed * 2 + 1), fb_(cfg.fault_b, cfg.seed * 2 + 2)
{
    sent_next_.store(1);
}

void MoldFeedSender::schedule_both(const uint8_t *data, size_t len, uint64_t now_ns)
{
    std::vector<FaultInjector::Delivery> out;
    fa_.apply(data, len, now_ns, out);
    for (auto &d : out) { heap_.push_back({d.due_ns, order_++, 0, std::move(d.bytes)}); std::push_heap(heap_.begin(), heap_.end(), std::greater<Pending>()); }
    out.clear();
    fb_.apply(data, len, now_ns, out);
    for (auto &d : out) { heap_.push_back({d.due_ns, order_++, 1, std::move(d.bytes)}); std::push_heap(heap_.begin(), heap_.end(), std::greater<Pending>()); }
}

void MoldFeedSender::transmit(const std::vector<uint8_t> &pkt, uint64_t now_ns)
{
    mold::Packet p;
    if (mold::parse_packet(pkt.data(), pkt.size(), p) == mold::ParseError::None) sent_next_.store(p.end_sequence());   // what a re-request may now ask for
    ++packets_;
    last_send_ns_ = now_ns;
    schedule_both(pkt.data(), pkt.size(), now_ns);
}

void MoldFeedSender::publish(const uint8_t *msg, size_t len, uint64_t now_ns)
{
    log_.append(msg, len);
    std::vector<uint8_t> pkt;
    if (packetizer_.add(msg, len, pkt)) transmit(pkt, now_ns);
}

void MoldFeedSender::flush(uint64_t now_ns)
{
    std::vector<uint8_t> pkt;
    if (packetizer_.flush(pkt)) transmit(pkt, now_ns);
}

void MoldFeedSender::end_session(uint64_t now_ns)
{
    if (ended_) return;
    flush(now_ns);
    ended_ = true;
    eos_started_ns_ = now_ns;
    last_eos_ns_ = 0;
}

void MoldFeedSender::service(uint64_t now_ns)
{
    uint8_t hb[mold::kHeaderSize];
    if (ended_ && (last_eos_ns_ == 0 || now_ns - last_eos_ns_ >= cfg_.end_of_session_interval_ns) && now_ns - eos_started_ns_ < cfg_.end_of_session_tail_ns)
    {
        last_eos_ns_ = now_ns;
        mold::build_end_of_session(hb, cfg_.session, log_.next_sequence());
        schedule_both(hb, sizeof hb, now_ns);
    }
    else if (!ended_ && now_ns - last_send_ns_ >= cfg_.heartbeat_interval_ns)
    {
        last_send_ns_ = now_ns;
        // a heartbeat advertises the next sequence the transmitter will use, counting only packets already sent
        mold::build_heartbeat(hb, cfg_.session, sent_next_.load());
        schedule_both(hb, sizeof hb, now_ns);
    }
    while (!heap_.empty() && heap_.front().due_ns <= now_ns)
    {
        std::pop_heap(heap_.begin(), heap_.end(), std::greater<Pending>());
        Pending p = std::move(heap_.back());
        heap_.pop_back();
        line_[p.line]->send(p.bytes.data(), p.bytes.size());
    }
}

bool MoldFeedSender::finished(uint64_t now_ns) const
{
    return ended_ && heap_.empty() && now_ns - eos_started_ns_ >= cfg_.end_of_session_tail_ns;
}

}  // namespace feed_handler::feed

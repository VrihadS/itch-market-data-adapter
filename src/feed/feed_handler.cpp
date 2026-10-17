#include "itch/feed/feed_handler.hpp"

#include <chrono>
#include <future>
#include <spdlog/spdlog.h>

#include "itch/net/moldudp64.hpp"

namespace feed_handler::feed
{

namespace
{
uint64_t SteadyNowNs()
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}
} // namespace

// Feeds datagrams from one socket into the session, tagged with the line they arrived on.
struct FeedHandler::LineCallback : SocketCallback
{
    LineCallback(FeedHandler &h, Line l) : handler(h), line(l) {}
    void OnDatagram(const char *data, size_t len, const RxInfo &info) override
    {
        if (!handler.running_.load(std::memory_order_relaxed)) return;
        if (line == Line::Rerequest && handler.server_ip_ != 0 && info.source_ip != handler.server_ip_)
        {
            handler.foreign_.fetch_add(1, std::memory_order_relaxed);    // replies must come from the re-request server
            return;
        }
        handler.session_->on_packet(line, reinterpret_cast<const uint8_t *>(data), len, info.rx_ns);
    }
    void OnDisconnect() override { spdlog::error("feed line {} socket failed", to_string(line)); }
    FeedHandler &handler;
    Line line;
};

// Sends MoldUDP64 Request Packets from the same socket the replies come back to.
class FeedHandler::RerequestChannel : public IRerequestSender
{
public:
    RerequestChannel(std::shared_ptr<UnicastUDPSocket> *socket, socket_address server) : socket_(socket), server_(std::move(server)) {}
    bool send_request(const mold::Session &s, uint64_t first, uint16_t count) override
    {
        if (server_.ip.empty() || !*socket_ || !(*socket_)->IsRunning()) return false;
        uint8_t pkt[mold::kHeaderSize];
        mold::build_request(pkt, s, first, count);
        return (*socket_)->SendToSync(reinterpret_cast<const char *>(pkt), sizeof pkt, server_);   // called on the network thread
    }
private:
    std::shared_ptr<UnicastUDPSocket> *socket_;
    socket_address server_;
};

FeedHandler::FeedHandler(SocketManager &sockets, FeedHandlerConfig cfg, SpscRing &ring)
    : sockets_(sockets), cfg_(std::move(cfg)), sink_(ring)
{
    rerequest_ = std::make_unique<RerequestChannel>(&r_, cfg_.rerequest_server);
    session_ = std::make_unique<FeedSession>(cfg_.feed, sink_, *rerequest_);
    cb_a_ = std::make_unique<LineCallback>(*this, Line::A);
    cb_b_ = std::make_unique<LineCallback>(*this, Line::B);
    cb_r_ = std::make_unique<LineCallback>(*this, Line::Rerequest);
    if (!cfg_.rerequest_server.ip.empty())
    {
        boost::system::error_code ec;
        const auto a = boost::asio::ip::make_address_v4(cfg_.rerequest_server.ip, ec);
        if (!ec) server_ip_ = a.to_uint();
    }
}

FeedHandler::~FeedHandler() { Stop(); }

bool FeedHandler::Start(std::string &error)
{
    if (running_.load()) return true;
    if (!sockets_.IsStarted()) { error = "SocketManager is not started"; return false; }
    if (cfg_.rerequest_server.ip.empty())
        spdlog::warn("no re-request server configured: a lost packet that line B does not have cannot be recovered");

    a_ = sockets_.CreateUDPSocket(cfg_.line_a, cb_a_.get(), cfg_.udp);
    b_ = sockets_.CreateUDPSocket(cfg_.line_b, cb_b_.get(), cfg_.udp);
    if (!cfg_.rerequest_server.ip.empty())
    {
        UdpOptions uo = cfg_.udp;
        uo.reuse_port = false;
        r_ = sockets_.CreateUDPUnicastSocket({"", cfg_.rerequest_local_port}, cb_r_.get(), uo);
    }
    if (!a_ || !b_ || (!cfg_.rerequest_server.ip.empty() && !r_)) { error = "could not create sockets"; return false; }

    running_.store(true);
    a_->Start();
    b_->Start();
    if (r_) r_->Start();
    const bool ok = a_->WaitUntilStarted() && b_->WaitUntilStarted() && (!r_ || r_->WaitUntilStarted());
    if (!ok)
    {
        error = !a_->IsRunning() ? "line A: " + a_->LastError() : !b_->IsRunning() ? "line B: " + b_->LastError() : "re-request socket: " + r_->LastError();
        Stop();
        return false;
    }
    auto started = std::make_shared<std::promise<void>>();
    auto fut = started->get_future();
    sockets_.Post([this, started] {
        timer_ = std::make_unique<boost::asio::steady_timer>(sockets_.GetIOContext());
        ScheduleTick();
        started->set_value();
    });
    if (fut.wait_for(std::chrono::seconds(2)) != std::future_status::ready) { error = "network thread not responding"; Stop(); return false; }
    return true;
}

void FeedHandler::ScheduleTick()
{
    if (!running_.load(std::memory_order_relaxed) || !timer_) return;
    timer_->expires_after(std::chrono::nanoseconds(cfg_.tick_interval_ns));
    timer_->async_wait([this](const boost::system::error_code &ec) {
        if (ec || !running_.load(std::memory_order_relaxed)) return;
        session_->tick(SteadyNowNs());
        ScheduleTick();
    });
}

void FeedHandler::RequestRebuild()
{
    sockets_.Post([this] { if (running_.load()) session_->rebuild(SteadyNowNs()); });
}

void FeedHandler::Stop()
{
    const bool was_running = running_.exchange(false);
    if (!was_running && !a_ && !b_ && !r_) return;
    if (sockets_.IsStarted())
    {
        auto cancelled = std::make_shared<std::promise<void>>();
        auto fut = cancelled->get_future();
        if (sockets_.Post([this, cancelled] { if (timer_) timer_->cancel(); cancelled->set_value(); })) fut.wait_for(std::chrono::seconds(2));
    }
    if (a_) a_->StopAndWait();
    if (b_) b_->StopAndWait();
    if (r_) r_->StopAndWait();
    a_.reset();
    b_.reset();
    r_.reset();
}

uint64_t FeedHandler::rerequest_bytes_received() const { return r_ ? r_->Stats().bytes.load() : 0; }

}  // namespace feed_handler::feed

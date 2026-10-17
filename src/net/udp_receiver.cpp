#include "itch/net/udp_receiver.h"

#include <algorithm>
#include <future>
#include <spdlog/spdlog.h>

#include <sys/socket.h>

namespace feed_handler
{

namespace
{
uint64_t SteadyNowNs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}
} // namespace

UdpReceiver::UdpReceiver(const socket_address &bind, SocketCallback *callback, std::shared_ptr<WorkerThread> thread,
                         const UdpOptions &options, const char *tag)
    : address_(bind), callback_(callback), thread_(std::move(thread)), options_(options), tag_(tag)
{
}

UdpReceiver::~UdpReceiver() = default;

std::string UdpReceiver::LastError() const
{
    std::lock_guard<std::mutex> g(mu_);
    return last_error_;
}

void UdpReceiver::SetState(State s)
{
    {
        std::lock_guard<std::mutex> g(mu_);
        state_.store(s, std::memory_order_release);
    }
    cv_.notify_all();
}

bool UdpReceiver::Configure(boost::asio::ip::udp::socket &, std::string &) { return true; }

boost::asio::ip::address_v4 UdpReceiver::BindAddress() const
{
    if (address_.ip.empty()) return boost::asio::ip::address_v4::any();
    boost::system::error_code ec;
    const auto a = boost::asio::ip::make_address_v4(address_.ip, ec);
    return ec ? boost::asio::ip::address_v4::any() : a;
}

void UdpReceiver::Start()
{
    State expected = State::Idle;
    if (!state_.compare_exchange_strong(expected, State::Starting)) return;   // already started (or stopped for good)
    auto self = shared_from_this();
    boost::asio::post(thread_->GetIOContext(), [self] { self->DoStart(); });
}

void UdpReceiver::Stop()
{
    auto self = shared_from_this();
    boost::asio::post(thread_->GetIOContext(), [self] { self->DoStop(); });
}

bool UdpReceiver::StopAndWait(std::chrono::milliseconds timeout)
{
    const State st = GetState();
    if (st == State::Idle) { SetState(State::Stopped); return true; }
    if (st == State::Stopped || st == State::Failed) return true;
    if (thread_->IsWorkerThread()) { DoStop(); return true; }   // waiting on ourselves would deadlock
    auto done = std::make_shared<std::promise<void>>();
    auto fut = done->get_future();
    auto self = shared_from_this();
    boost::asio::post(thread_->GetIOContext(), [self, done] { self->DoStop(); done->set_value(); });
    return fut.wait_for(timeout) == std::future_status::ready;
}

bool UdpReceiver::WaitUntilStarted(std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> g(mu_);
    cv_.wait_for(g, timeout, [&] {
        const State s = state_.load();
        if (s == State::Running) return connect_done_;
        return s != State::Starting && s != State::Idle;
    });
    return state_.load() == State::Running && connect_done_;
}

void UdpReceiver::Fail(const std::string &why)
{
    {
        std::lock_guard<std::mutex> g(mu_);
        last_error_ = why;
    }
    spdlog::error("{}: {}", tag_, why);
    boost::system::error_code ec;
    if (retry_timer_) retry_timer_->cancel();
    if (socket_ && socket_->is_open()) socket_->close(ec);
    SetState(State::Failed);
    if (callback_)
    {
        try { callback_->OnDisconnect(); }
        catch (const std::exception &e) { spdlog::error("{}: OnDisconnect threw: {}", tag_, e.what()); }
    }
}

void UdpReceiver::DoStart()
{
    namespace ip = boost::asio::ip;
    if (GetState() != State::Starting) return;   // stopped before it began
    boost::system::error_code ec;

    socket_ = std::make_unique<ip::udp::socket>(thread_->GetIOContext());
    socket_->open(ip::udp::v4(), ec);
    if (ec) return Fail("open: " + ec.message());

    socket_->set_option(boost::asio::socket_base::reuse_address(true), ec);
#ifdef SO_REUSEPORT
    if (options_.reuse_port)
    {
        const int on = 1;
        ::setsockopt(socket_->native_handle(), SOL_SOCKET, SO_REUSEPORT, &on, sizeof on);
    }
#endif
    socket_->set_option(boost::asio::socket_base::receive_buffer_size(options_.rcvbuf_bytes), ec);
    if (ec) spdlog::warn("{}: could not set SO_RCVBUF to {}: {}", tag_, options_.rcvbuf_bytes, ec.message());

    socket_->bind(ip::udp::endpoint(BindAddress(), address_.port), ec);
    if (ec)
    {
        ip::address_v4 alt;
        if (BindFallback(alt))
        {
            spdlog::warn("{}: bind to {} failed ({}); binding to {} instead", tag_, BindAddress().to_string(), ec.message(), alt.to_string());
            socket_->bind(ip::udp::endpoint(alt, address_.port), ec);
        }
    }
    if (ec) return Fail("bind " + (address_.ip.empty() ? std::string("0.0.0.0") : address_.ip) + ":" + std::to_string(address_.port) + ": " + ec.message());

    std::string error;
    if (!Configure(*socket_, error)) return Fail(error);

    socket_->non_blocking(true, ec);
    if (ec) return Fail("non_blocking: " + ec.message());

    boost::asio::socket_base::receive_buffer_size actual;
    socket_->get_option(actual, ec);
    if (!ec)
    {
        effective_rcvbuf_.store(actual.value(), std::memory_order_release);
        // Linux reports double the requested size; anything below half of it means the OS capped the request
        // (net.core.rmem_max / kern.ipc.maxsockbuf) and bursts will be dropped by the kernel before we see them.
        if (actual.value() < options_.rcvbuf_bytes / 2)
            spdlog::warn("{}: receive buffer is {} bytes, {} requested; raise net.core.rmem_max (Linux) or kern.ipc.maxsockbuf (macOS)",
                         tag_, actual.value(), options_.rcvbuf_bytes);
    }
    const auto local = socket_->local_endpoint(ec);
    if (!ec) local_port_.store(local.port(), std::memory_order_release);

    retry_timer_ = std::make_unique<boost::asio::steady_timer>(thread_->GetIOContext());
    SetState(State::Running);
    spdlog::info("{}: receiving on {}:{} (rcvbuf {} bytes)", tag_, address_.ip.empty() ? "0.0.0.0" : address_.ip, LocalPort(), EffectiveRcvBuf());
    if (callback_)
    {
        try { callback_->OnConnect(); }
        catch (const std::exception &e) { spdlog::error("{}: OnConnect threw: {}", tag_, e.what()); }
    }
    {
        std::lock_guard<std::mutex> g(mu_);
        connect_done_ = true;               // WaitUntilStarted() returns only after OnConnect() has run
    }
    cv_.notify_all();
    ArmWait();
}

void UdpReceiver::DoStop()
{
    boost::system::error_code ec;
    if (retry_timer_) retry_timer_->cancel();
    if (socket_ && socket_->is_open()) socket_->close(ec);
    const State st = GetState();
    if (st != State::Failed) SetState(State::Stopped);
}

void UdpReceiver::ArmWait()
{
    if (GetState() != State::Running) return;
    auto self = shared_from_this();
    socket_->async_wait(boost::asio::ip::udp::socket::wait_read, [self](const boost::system::error_code &ec) { self->OnReadable(ec); });
}

void UdpReceiver::OnReadable(const boost::system::error_code &wait_ec)
{
    if (wait_ec == boost::asio::error::operation_aborted || GetState() != State::Running) return;
    if (wait_ec) { HandleError(wait_ec); return; }

    stats_.wakeups.store(stats_.wakeups.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    uint32_t batch = 0;
    for (; batch < options_.max_drain; ++batch)
    {
        boost::system::error_code ec;
        const size_t n = socket_->receive_from(boost::asio::buffer(buf_), sender_, 0, ec);
        if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) break;   // drained
        if (ec == boost::asio::error::operation_aborted || GetState() != State::Running) return;
        if (ec) { HandleError(ec); return; }

        consecutive_errors_ = 0;
        const uint64_t now = SteadyNowNs();
        stats_.packets.store(stats_.packets.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
        stats_.bytes.store(stats_.bytes.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
        stats_.last_rx_ns.store(now, std::memory_order_relaxed);
        if (callback_)
        {
            RxInfo info;
            info.rx_ns = now;
            info.source_ip = sender_.address().is_v4() ? sender_.address().to_v4().to_uint() : 0;
            info.source_port = sender_.port();
            try { callback_->OnDatagram(buf_.data(), n, info); }
            catch (const std::exception &e) { spdlog::error("{}: callback threw: {}", tag_, e.what()); }
            catch (...) { spdlog::error("{}: callback threw a non-standard exception", tag_); }
            if (GetState() != State::Running) return;   // the callback stopped us
        }
    }
    if (batch > stats_.max_batch.load(std::memory_order_relaxed)) stats_.max_batch.store(batch, std::memory_order_relaxed);
    ArmWait();
}

void UdpReceiver::HandleError(const boost::system::error_code &ec)
{
    const uint64_t errs = stats_.errors.load(std::memory_order_relaxed) + 1;
    stats_.errors.store(errs, std::memory_order_relaxed);
    ++consecutive_errors_;
    if (errs == 1 || errs % 1000 == 0) spdlog::warn("{}: receive error: {} (total {})", tag_, ec.message(), errs);
    if (consecutive_errors_ > options_.max_consecutive_errors)
    {
        Fail("giving up after " + std::to_string(consecutive_errors_) + " consecutive receive errors, last: " + ec.message());
        return;
    }
    // Back off so a persistent error cannot spin the network thread.
    const auto delay = std::chrono::milliseconds(std::min<uint32_t>(100, 1u << std::min<uint32_t>(consecutive_errors_, 7)));
    retry_timer_->expires_after(delay);
    auto self = shared_from_this();
    retry_timer_->async_wait([self](const boost::system::error_code &e) {
        if (e != boost::asio::error::operation_aborted) self->ArmWait();
    });
}

}   // namespace feed_handler

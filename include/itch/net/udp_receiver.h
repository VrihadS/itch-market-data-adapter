#pragma once
// Shared receive engine for the UDP sockets (multicast and unicast).
//
//   - runs entirely on the SocketManager's network thread (Boost.Asio reactor)
//   - waits for readability, then drains up to UdpOptions::max_drain datagrams per wakeup
//   - errors back off (1 ms doubling to 100 ms) instead of spinning; after max_consecutive_errors the
//     socket is declared failed and SocketCallback::OnDisconnect() is called once
//   - StopAndWait() guarantees that no callback runs after it returns
//
// Lifetime: sockets are created through SocketManager as shared_ptr; every pending handler holds a
// reference, so a socket cannot disappear under the network thread. The SocketCallback you pass must
// outlive the socket's activity: call StopAndWait() before destroying it.

#include <atomic>
#include <array>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include <boost/asio.hpp>

#include "itch/net/socket_common.h"
#include "itch/net/worker_thread.h"

namespace feed_handler
{

class UdpReceiver : public std::enable_shared_from_this<UdpReceiver>
{
public:
    enum class State : int { Idle, Starting, Running, Failed, Stopped };

    virtual ~UdpReceiver();
    UdpReceiver(const UdpReceiver &) = delete;
    UdpReceiver &operator=(const UdpReceiver &) = delete;

    void Start();   // asynchronous: opens, binds, joins and starts receiving on the network thread
    void Stop();    // asynchronous close
    // Closes and waits until it has happened. Returns false if the network thread did not get to it in time
    // (for instance because it is not running).
    bool StopAndWait(std::chrono::milliseconds timeout = std::chrono::milliseconds(2000));
    // True once the socket is bound and receiving; false if it failed or the wait timed out.
    bool WaitUntilStarted(std::chrono::milliseconds timeout = std::chrono::milliseconds(2000));

    State       GetState() const { return state_.load(std::memory_order_acquire); }
    bool        IsRunning() const { return GetState() == State::Running; }
    std::string LastError() const;
    uint16_t    LocalPort() const { return local_port_.load(std::memory_order_acquire); }
    int         EffectiveRcvBuf() const { return effective_rcvbuf_.load(std::memory_order_acquire); }
    const SocketStats &Stats() const { return stats_; }

protected:
    UdpReceiver(const socket_address &bind, SocketCallback *callback, std::shared_ptr<WorkerThread> thread,
                const UdpOptions &options, const char *tag);

    // Hook called on the network thread after bind(): join multicast groups etc. Return false and set `error` to fail.
    virtual bool Configure(boost::asio::ip::udp::socket &sock, std::string &error);
    // Address to bind to; default is the configured ip, or any if empty.
    virtual boost::asio::ip::address_v4 BindAddress() const;
    // If bind() to BindAddress() fails, an address to try instead (return false for none).
    virtual bool BindFallback(boost::asio::ip::address_v4 & /*alternative*/) const { return false; }

    boost::asio::ip::udp::socket &Sock() { return *socket_; }   // network thread only
    const socket_address &Address() const { return address_; }
    const UdpOptions &Options() const { return options_; }
    std::shared_ptr<WorkerThread> Thread() const { return thread_; }
    SocketStats &MutableStats() { return stats_; }
    const char *Tag() const { return tag_; }

private:
    void DoStart();
    void DoStop();
    void Fail(const std::string &why);
    void ArmWait();
    void OnReadable(const boost::system::error_code &ec);
    void HandleError(const boost::system::error_code &ec);
    void SetState(State s);

    socket_address                address_;
    SocketCallback               *callback_;
    std::shared_ptr<WorkerThread> thread_;
    UdpOptions                    options_;
    const char                   *tag_;

    std::unique_ptr<boost::asio::ip::udp::socket>      socket_;
    std::unique_ptr<boost::asio::steady_timer>          retry_timer_;
    boost::asio::ip::udp::endpoint                      sender_;
    std::array<char, 65536>                             buf_;

    std::atomic<State> state_{State::Idle};
    std::atomic<uint16_t> local_port_{0};
    std::atomic<int> effective_rcvbuf_{0};
    uint32_t consecutive_errors_ = 0;
    bool connect_done_ = false;          // OnConnect() has returned (guarded by mu_)
    SocketStats stats_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::string last_error_;
};

}   // namespace feed_handler

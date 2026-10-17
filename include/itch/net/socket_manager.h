#pragma once

#include <functional>
#include <memory>

#include "itch/net/socket_common.h"
#include "itch/net/udp_socket.h"
#include "itch/net/unicast_udp_socket.h"
#include "itch/net/worker_thread.h"

namespace feed_handler
{

// Owns the network thread (one Boost.Asio reactor) and creates the sockets that run on it.
//
//   SocketManager m;  m.Init(cpu);  auto a = m.CreateUDPSocket({"239.1.1.1", 30001}, &cb, {.interface_ip = "10.0.0.5"});
//   m.Start();        a->Start();   ...   a->StopAndWait();   m.Stop();
//
// Every socket callback runs on the network thread, so a callback never races with another callback of
// any socket from the same manager. Create sockets after Init(); a null return means Init() has not run.
class SocketManager
{
  public:
    struct NetworkThreadConfig
    {
        int            cpu         = -1;                          // pin to this core; -1 = unpinned
        WorkerPollMode poll_mode   = WorkerPollMode::Default;     // Default: pinned -> busy-spin, unpinned -> block
        int            spin_window = 100;                         // SpinThenPark only
        std::string    name        = "sck_net";
    };

    SocketManager();
    ~SocketManager();

    SocketManager(const SocketManager &)            = delete;
    SocketManager &operator=(const SocketManager &) = delete;

    error_code Init(int cpu = -1, TCPMode mode = TCPMode::Standard);
    error_code Init(const NetworkThreadConfig &config, TCPMode mode = TCPMode::Standard);
    error_code Start();   // idempotent; error_code::failed_to_initialize before Init()
    void       Stop();    // joins the network thread; sockets stop receiving
    bool       IsStarted() const { return started_; }

    // Multicast (or unicast-bound) receive socket. `address.ip` a multicast group => joins it.
    std::shared_ptr<UDPSocket> CreateUDPSocket(const socket_address &address, SocketCallback *callback, const UdpOptions &options = {});
    // Unicast socket that can also send (re-request channel).
    std::shared_ptr<UnicastUDPSocket> CreateUDPUnicastSocket(const socket_address &bind_address, SocketCallback *callback,
                                                             const UdpOptions &options = {});

    // Runs `fn` on the network thread (from any thread). False if the manager is not initialized.
    bool Post(std::function<void()> fn);
    boost::asio::io_context *TryGetIOContext() { return network_thread_ ? &network_thread_->GetIOContext() : nullptr; }
    boost::asio::io_context &GetIOContext() { return network_thread_->GetIOContext(); }   // requires Init()
    std::shared_ptr<WorkerThread> NetworkThread() const { return network_thread_; }

  private:
    TCPMode                       mode_ = TCPMode::Standard;
    std::shared_ptr<WorkerThread> network_thread_;
    bool                          started_ = false;
};

} // namespace feed_handler

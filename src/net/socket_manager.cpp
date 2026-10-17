#include "itch/net/socket_manager.h"

#include <spdlog/spdlog.h>

namespace feed_handler
{

SocketManager::SocketManager() = default;

SocketManager::~SocketManager() { Stop(); }

error_code SocketManager::Init(int cpu, TCPMode mode)
{
    NetworkThreadConfig cfg;
    cfg.cpu = cpu;
    return Init(cfg, mode);
}

error_code SocketManager::Init(const NetworkThreadConfig &config, TCPMode mode)
{
    if (network_thread_) return error_code::already_started;
    mode_ = mode;
    network_thread_ = std::make_shared<WorkerThread>(config.name, static_cast<int16_t>(config.cpu), config.poll_mode, config.spin_window);
    return error_code::ok;
}

error_code SocketManager::Start()
{
    if (!network_thread_) return error_code::failed_to_initialize;
    if (started_) return error_code::ok;
    network_thread_->Start();
    started_ = true;
    return error_code::ok;
}

void SocketManager::Stop()
{
    if (!network_thread_ || !started_) return;
    network_thread_->Stop();
    started_ = false;
}

std::shared_ptr<UDPSocket> SocketManager::CreateUDPSocket(const socket_address &address, SocketCallback *callback, const UdpOptions &options)
{
    if (!network_thread_)
    {
        spdlog::error("SocketManager::CreateUDPSocket before Init()");
        return nullptr;
    }
    if (address.port == 0 && !address.ip.empty())
    {
        boost::system::error_code ec;
        if (boost::asio::ip::make_address_v4(address.ip, ec).is_multicast())
        {
            spdlog::error("SocketManager::CreateUDPSocket: multicast group {} needs a port", address.ip);
            return nullptr;
        }
    }
    return std::make_shared<UDPSocket>(address, callback, network_thread_, options);
}

std::shared_ptr<UnicastUDPSocket> SocketManager::CreateUDPUnicastSocket(const socket_address &bind_address, SocketCallback *callback,
                                                                         const UdpOptions &options)
{
    if (!network_thread_)
    {
        spdlog::error("SocketManager::CreateUDPUnicastSocket before Init()");
        return nullptr;
    }
    return std::make_shared<UnicastUDPSocket>(bind_address, callback, network_thread_, options);
}

bool SocketManager::Post(std::function<void()> fn)
{
    if (!network_thread_) return false;
    boost::asio::post(network_thread_->GetIOContext(), std::move(fn));
    return true;
}

} // namespace feed_handler

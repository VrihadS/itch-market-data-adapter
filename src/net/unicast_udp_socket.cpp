#include "itch/net/unicast_udp_socket.h"

#include <spdlog/spdlog.h>

namespace feed_handler
{

UnicastUDPSocket::UnicastUDPSocket(const socket_address &bind_address, SocketCallback *callback,
                                   const std::shared_ptr<WorkerThread> &network_thread, const UdpOptions &options)
    : UdpReceiver(bind_address, callback, network_thread, options, "UnicastUDPSocket")
{
}

void UnicastUDPSocket::SendTo(const char *data, std::size_t len, const socket_address &dest)
{
    auto self = std::static_pointer_cast<UnicastUDPSocket>(shared_from_this());
    boost::asio::post(Thread()->GetIOContext(), [self, buf = std::vector<char>(data, data + len), dest]() {
        self->SendToSync(buf.data(), buf.size(), dest);
    });
}

bool UnicastUDPSocket::SendToSync(const char *data, std::size_t len, const socket_address &dest)
{
    if (!IsRunning())
    {
        spdlog::warn("UnicastUDPSocket: send before the socket is running");
        MutableStats().send_errors.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    boost::system::error_code ec;
    const auto addr = boost::asio::ip::make_address_v4(dest.ip, ec);
    if (ec)
    {
        spdlog::error("UnicastUDPSocket: bad destination '{}': {}", dest.ip, ec.message());
        MutableStats().send_errors.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    Sock().send_to(boost::asio::buffer(data, len), boost::asio::ip::udp::endpoint(addr, dest.port), 0, ec);
    if (ec)
    {
        spdlog::error("UnicastUDPSocket: send_to {}:{}: {}", dest.ip, dest.port, ec.message());
        MutableStats().send_errors.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

}   // namespace feed_handler

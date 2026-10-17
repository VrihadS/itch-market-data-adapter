#pragma once
// UDP socket for market data lines. If `address.ip` is a multicast group (224.0.0.0/4) the socket binds
// to the port, joins the group on UdpOptions::interface_ip and receives it; otherwise it binds to the
// given unicast address (empty = any). Created through SocketManager::CreateUDPSocket.
#include "itch/net/udp_receiver.h"

namespace feed_handler
{

class UDPSocket : public UdpReceiver
{
public:
    UDPSocket(const socket_address &address, SocketCallback *callback, std::shared_ptr<WorkerThread> network_thread,
              const UdpOptions &options = {});

    bool IsMulticast() const { return multicast_; }

protected:
    bool Configure(boost::asio::ip::udp::socket &sock, std::string &error) override;
    boost::asio::ip::address_v4 BindAddress() const override;
    bool BindFallback(boost::asio::ip::address_v4 &alternative) const override;

private:
    bool multicast_ = false;
    boost::asio::ip::address_v4 group_;
};

}   // namespace feed_handler

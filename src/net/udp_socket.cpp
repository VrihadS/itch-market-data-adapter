#include "itch/net/udp_socket.h"

namespace feed_handler
{

UDPSocket::UDPSocket(const socket_address &address, SocketCallback *callback, std::shared_ptr<WorkerThread> network_thread,
                     const UdpOptions &options)
    : UdpReceiver(address, callback, std::move(network_thread), options, "UDPSocket")
{
    boost::system::error_code ec;
    const auto a = boost::asio::ip::make_address_v4(address.ip, ec);
    if (!ec && a.is_multicast())
    {
        multicast_ = true;
        group_ = a;
    }
}

boost::asio::ip::address_v4 UDPSocket::BindAddress() const
{
    // Bound to the group address, only that group's traffic reaches this socket; bound to the wildcard it
    // would also get every other group joined on the same port by anything on the host.
    if (multicast_) return Options().bind_to_group ? group_ : boost::asio::ip::address_v4::any();
    return UdpReceiver::BindAddress();
}

bool UDPSocket::BindFallback(boost::asio::ip::address_v4 &alternative) const
{
    if (!multicast_ || !Options().bind_to_group) return false;
    alternative = boost::asio::ip::address_v4::any();     // the OS refused to bind to the group address
    return true;
}

bool UDPSocket::Configure(boost::asio::ip::udp::socket &sock, std::string &error)
{
    if (!multicast_) return true;
    boost::system::error_code ec;
    boost::asio::ip::address_v4 iface = boost::asio::ip::address_v4::any();
    if (!Options().interface_ip.empty())
    {
        iface = boost::asio::ip::make_address_v4(Options().interface_ip, ec);
        if (ec) { error = "bad interface address '" + Options().interface_ip + "'"; return false; }
    }
    sock.set_option(boost::asio::ip::multicast::join_group(group_, iface), ec);
    if (ec)
    {
        error = "join " + group_.to_string() + " on " + (Options().interface_ip.empty() ? std::string("default interface") : Options().interface_ip) + ": " + ec.message();
        return false;
    }
    sock.set_option(boost::asio::ip::multicast::enable_loopback(Options().multicast_loopback), ec);
    return true;
}

}   // namespace feed_handler

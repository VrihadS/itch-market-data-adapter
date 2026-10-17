#pragma once
// Unicast UDP socket for send + receive without joining a multicast group.
// Used to send MoldUDP64 Request Packets to a Re-request Server and receive its unicast replies:
// the server answers to the request's source address, so replies arrive on this same socket.
//
// bind_address controls the local endpoint:
//   {ip="", port=0}   — bind to 0.0.0.0 on a kernel-assigned ephemeral port (see LocalPort())
//   {ip="", port=N}   — bind to 0.0.0.0:N
#include "itch/net/udp_receiver.h"

namespace feed_handler
{

class UnicastUDPSocket : public UdpReceiver
{
public:
    UnicastUDPSocket(const socket_address &bind_address, SocketCallback *callback,
                     const std::shared_ptr<WorkerThread> &network_thread, const UdpOptions &options = {});

    // Send to a remote unicast address from any thread (the data is copied and sent on the network thread).
    void SendTo(const char *data, std::size_t len, const socket_address &dest);

    // Synchronous variant — call only from the network thread. Returns false if the datagram was not sent.
    bool SendToSync(const char *data, std::size_t len, const socket_address &dest);
};

}   // namespace feed_handler

// The socket layer over real loopback UDP: receive engine, batching, multicast, isolation, lifecycle, failure.
#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <spdlog/spdlog.h>

#include "check.h"
#include "itch/feed/mold_sender.hpp"
#include "itch/net/socket_manager.h"

using namespace feed_handler;
using namespace std::chrono_literals;

struct Collector : SocketCallback {
  std::mutex mu; std::vector<std::vector<uint8_t>> got; std::vector<RxInfo> infos; std::atomic<int> disconnects{0}, connects{0};
  std::atomic<bool> throw_next{false}; std::atomic<int> slow_first_ms{0};
  void OnDatagram(const char *d, size_t n, const RxInfo &i) override {
    if (slow_first_ms.exchange(0) > 0) std::this_thread::sleep_for(50ms);
    if (throw_next.exchange(false)) throw std::runtime_error("boom");
    std::lock_guard<std::mutex> g(mu); got.emplace_back(d, d + n); infos.push_back(i); }
  void OnConnect() override { ++connects; }
  void OnDisconnect() override { ++disconnects; }
  size_t count() { std::lock_guard<std::mutex> g(mu); return got.size(); }
  bool wait_for(size_t n, std::chrono::milliseconds t = 3000ms) { const auto end = std::chrono::steady_clock::now() + t; while (std::chrono::steady_clock::now() < end) { if (count() >= n) return true; std::this_thread::sleep_for(1ms); } return count() >= n; }
};

static std::vector<uint8_t> payload(uint32_t i) { std::vector<uint8_t> b(8 + (i % 40)); std::memcpy(b.data(), &i, 4); for (size_t k = 4; k < b.size(); ++k) b[k] = (uint8_t)(i * 31 + k); return b; }

int main() {
  spdlog::set_level(spdlog::level::off);
  const unsigned base = 41000 + (unsigned)(::getpid() % 2000) * 8;

  { // misuse is reported, not crashed on
    SocketManager m; Collector c;
    CHECK(m.CreateUDPSocket({"127.0.0.1", 1}, &c) == nullptr && m.CreateUDPUnicastSocket({"", 0}, &c) == nullptr);
    CHECK(m.Start() == error_code::failed_to_initialize && m.TryGetIOContext() == nullptr && !m.Post([] {}));
    CHECK(m.Init() == error_code::ok && m.Init() == error_code::already_started);
    CHECK(m.CreateUDPSocket({"239.1.1.1", 0}, &c) == nullptr);                  // a multicast group needs a port
    CHECK(m.Start() == error_code::ok && m.Start() == error_code::ok && m.IsStarted());
    m.Stop(); m.Stop();
  }
  SocketManager m; CHECK(m.Init() == error_code::ok && m.Start() == error_code::ok);

  { // unicast receive: bytes, order, metadata
    Collector c; auto s = m.CreateUDPSocket({"127.0.0.1", 0}, &c); s->Start(); CHECK(s->WaitUntilStarted() && s->LocalPort() != 0 && c.connects == 1);
    CHECK(!s->IsMulticast() && s->EffectiveRcvBuf() > 0);
    feed::UdpSender tx; std::string err; CHECK(tx.open("127.0.0.1", s->LocalPort(), "", 1, err));
    for (uint32_t i = 0; i < 1000; ++i) { auto p = payload(i); CHECK(tx.send(p.data(), p.size())); if (i % 100 == 99) std::this_thread::sleep_for(1ms); }
    CHECK(c.wait_for(1000));
    for (uint32_t i = 0; i < 1000; ++i) CHECK(c.got[i] == payload(i));
    for (size_t i = 1; i < c.infos.size(); ++i) CHECK(c.infos[i].rx_ns >= c.infos[i - 1].rx_ns);
    CHECK(c.infos[0].source_ip == 0x7F000001u && c.infos[0].source_port != 0);
    CHECK(s->Stats().packets == 1000 && s->Stats().wakeups <= 1000 && s->Stats().last_rx_ns > 0 && s->Stats().errors == 0);
    CHECK(s->StopAndWait());
  }
  { // a burst arriving while the network thread is busy is drained in batches, not one wakeup per packet
    Collector c; c.slow_first_ms = 50; auto s = m.CreateUDPSocket({"127.0.0.1", 0}, &c); s->Start(); CHECK(s->WaitUntilStarted());
    feed::UdpSender tx; std::string err; CHECK(tx.open("127.0.0.1", s->LocalPort(), "", 1, err));
    for (uint32_t i = 0; i < 2000; ++i) { auto p = payload(i); tx.send(p.data(), p.size()); if (i % 200 == 199) std::this_thread::sleep_for(2ms); }
    CHECK(c.wait_for(2000));
    CHECK(s->Stats().max_batch >= 32 && s->Stats().wakeups < 1500);
    std::printf("  burst of 2000 datagrams: %llu wakeups, largest batch %llu\n", (unsigned long long)s->Stats().wakeups.load(), (unsigned long long)s->Stats().max_batch.load());
    CHECK(s->StopAndWait());
  }
  { // an exception in the callback does not stop the receiver
    Collector c; auto s = m.CreateUDPSocket({"127.0.0.1", 0}, &c); s->Start(); CHECK(s->WaitUntilStarted());
    feed::UdpSender tx; std::string err; CHECK(tx.open("127.0.0.1", s->LocalPort(), "", 1, err));
    c.throw_next = true; auto p = payload(1); tx.send(p.data(), p.size()); std::this_thread::sleep_for(20ms); tx.send(p.data(), p.size());
    CHECK(c.wait_for(1) && s->IsRunning()); CHECK(s->StopAndWait());
  }
  { // multicast: join on the loopback interface, receive, and lines on the same port are isolated by group
    Collector ca, cb; UdpOptions o; o.interface_ip = "127.0.0.1";
    const uint16_t port = (uint16_t)(base + 1);
    auto a = m.CreateUDPSocket({"239.77.1.1", port}, &ca, o), b = m.CreateUDPSocket({"239.77.1.2", port}, &cb, o);
    a->Start(); b->Start();
    if (!a->WaitUntilStarted() || !b->WaitUntilStarted()) {
      std::printf("  multicast: SKIPPED (%s) - not verified on this host\n", a->IsRunning() ? b->LastError().c_str() : a->LastError().c_str());
    } else {
      CHECK(a->IsMulticast());
      feed::UdpSender ta, tb; std::string err; CHECK(ta.open("239.77.1.1", port, "127.0.0.1", 1, err) && tb.open("239.77.1.2", port, "127.0.0.1", 1, err));
      for (uint32_t i = 0; i < 200; ++i) { auto p = payload(i); ta.send(p.data(), p.size()); }
      for (uint32_t i = 1000; i < 1100; ++i) { auto p = payload(i); tb.send(p.data(), p.size()); }
      const bool got = ca.wait_for(200) && cb.wait_for(100);
      if (!got) std::printf("  multicast: SKIPPED (joined but no loopback delivery: A=%zu B=%zu)\n", ca.count(), cb.count());
      else {
        std::this_thread::sleep_for(50ms);
        CHECK(ca.count() == 200 && cb.count() == 100);                      // no cross-delivery between the two lines
        for (uint32_t i = 0; i < 200; ++i) CHECK(ca.got[i] == payload(i));
        for (uint32_t i = 0; i < 100; ++i) CHECK(cb.got[i] == payload(1000 + i));
        std::puts("  multicast: ok (two groups on one port, no cross-delivery)");
      }
    }
    CHECK(a->StopAndWait() && b->StopAndWait());
  }
  { // StopAndWait: no callback runs afterwards
    Collector c; auto s = m.CreateUDPSocket({"127.0.0.1", 0}, &c); s->Start(); CHECK(s->WaitUntilStarted());
    feed::UdpSender tx; std::string err; CHECK(tx.open("127.0.0.1", s->LocalPort(), "", 1, err));
    auto p = payload(5); for (int i = 0; i < 50; ++i) tx.send(p.data(), p.size());
    CHECK(s->StopAndWait() && s->GetState() == UdpReceiver::State::Stopped);
    const size_t n = c.count(); for (int i = 0; i < 50; ++i) tx.send(p.data(), p.size()); std::this_thread::sleep_for(50ms);
    CHECK(c.count() == n && c.disconnects == 0);                              // an intentional stop is not a failure
    s->Start(); std::this_thread::sleep_for(20ms); CHECK(s->GetState() == UdpReceiver::State::Stopped);   // and it stays stopped
  }
  { // a bind failure is reported once, with the reason
    UdpOptions o; o.reuse_port = false; Collector c1, c2;
    auto s1 = m.CreateUDPSocket({"127.0.0.1", (uint16_t)(base + 2)}, &c1, o); s1->Start(); CHECK(s1->WaitUntilStarted());
    // SO_REUSEADDR alone lets some systems share a UDP port; use a privileged port to force a deterministic failure
    auto s2 = m.CreateUDPSocket({"127.0.0.1", 1}, &c2, o); s2->Start();
    CHECK(!s2->WaitUntilStarted() && s2->GetState() == UdpReceiver::State::Failed && c2.disconnects == 1);
    CHECK(s2->LastError().find("bind") != std::string::npos);
    CHECK(s1->StopAndWait());
  }
  { // two unicast sockets: send and receive, replies come back to the sender's own port
    Collector ca, cb; auto a = m.CreateUDPUnicastSocket({"127.0.0.1", 0}, &ca), b = m.CreateUDPUnicastSocket({"127.0.0.1", 0}, &cb);
    a->Start(); b->Start(); CHECK(a->WaitUntilStarted() && b->WaitUntilStarted());
    const char hello[] = "request"; a->SendTo(hello, sizeof hello, {"127.0.0.1", b->LocalPort()});
    CHECK(cb.wait_for(1) && cb.infos[0].source_port == a->LocalPort() && std::memcmp(cb.got[0].data(), hello, sizeof hello) == 0);
    const char reply[] = "reply"; b->SendTo(reply, sizeof reply, {"127.0.0.1", cb.infos[0].source_port});
    CHECK(ca.wait_for(1) && std::memcmp(ca.got[0].data(), reply, sizeof reply) == 0);
    CHECK(!a->SendToSync(hello, 1, {"not-an-ip", 5}) );                          // a bad destination is an error, not a crash (network thread only: this is a refusal path)
    CHECK(a->StopAndWait() && b->StopAndWait());
  }
  { // lifecycle stress: create, start, traffic, stop, destroy - many times (ASan clean)
    for (int i = 0; i < 300; ++i) {
      Collector c; auto s = m.CreateUDPSocket({"127.0.0.1", 0}, &c); s->Start(); CHECK(s->WaitUntilStarted());
      feed::UdpSender tx; std::string err; CHECK(tx.open("127.0.0.1", s->LocalPort(), "", 1, err)); auto p = payload(i); for (int k = 0; k < 5; ++k) tx.send(p.data(), p.size());
      if (i % 3 == 0) std::this_thread::sleep_for(1ms);
      CHECK(s->StopAndWait());
    }
  }
  m.Stop();
  std::puts("ok");
  return 0;
}

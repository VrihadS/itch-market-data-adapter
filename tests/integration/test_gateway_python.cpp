// The whole chain with the real Python client: embedded Aeron driver -> MarketDataServer -> Aeron IPC ->
// TcpGateway -> TCP -> `python3 clients/python/itch_md_client.py --json` as a subprocess.
// Exit code 77 means "skipped" (no Python interpreter).
#include <fcntl.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <spdlog/spdlog.h>

#include "check.h"
#include "itch_builders.h"
#include "itch/messaging/aeron_transport.hpp"
#include "itch/messaging/market_data_server.hpp"
#include "itch/messaging/tcp_gateway.hpp"

using namespace feed_handler;
using namespace feed_handler::messaging;
using namespace itchtest;
using Clock = std::chrono::steady_clock;

int main() {
  spdlog::set_level(spdlog::level::off);
#ifndef ITCH_PYTHON
  std::puts("SKIPPED: no Python interpreter found at configure time");
  return 77;
#else
  std::string err;
  auto driver = EmbeddedMediaDriver::start({}, err);                    CHECK(driver);
  auto server_tp = AeronTransport::connect(driver->dir(), err);         CHECK(server_tp);
  auto gw_tp = AeronTransport::connect(driver->dir(), err);             CHECK(gw_tp);

  BookManager books(1 << 12); NasdaqItchDecoder dec;
  auto feed = [&](const Bytes &b) { size_t u = 0; CHECK(dec.DecodeMessage((const char *)b.data(), b.size(), u, books)); };
  feed(MsgR(1, "AAPL"));
  feed(MsgA(1, 1, 'B', 10, "AAPL", 1000000)); feed(MsgA(1, 2, 'B', 20, "AAPL", 990000)); feed(MsgA(1, 3, 'S', 5, "AAPL", 1010000));

  ServerConfig sc; sc.control_udp_channel = "";                          // IPC only
  MarketDataServer server(*server_tp, books, sc);                       CHECK(server.start());
  GatewayConfig gc; gc.listen_port = 0; gc.upstream.server_host = "ipc";
  TcpGateway gateway(*gw_tp, gc);                                       CHECK(gateway.start(err));

  const std::string cmd = std::string(ITCH_PYTHON) + " " + ITCH_PY_CLIENT + " --host 127.0.0.1 --port " +
                          std::to_string(gateway.port()) + " --symbols AAPL --l2 3 --tns --seconds 5 --json 2>&1";
  FILE *child = ::popen(cmd.c_str(), "r");                              CHECK(child);
  const int fd = fileno(child);
  fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

  std::string out;
  bool fed = false, eof = false;
  const auto deadline = Clock::now() + std::chrono::seconds(20);
  while (!eof && Clock::now() < deadline) {
    server.poll_control();
    gateway.poll();
    char buf[8192];
    for (;;) {
      const ssize_t n = ::read(fd, buf, sizeof buf);
      if (n > 0) out.append(buf, n); else { if (n == 0) eof = true; break; }
    }
    if (!fed && out.find("\"snapshot\":true") != std::string::npos) {   // the client is subscribed: now move the market
      feed(WithTs(MsgA(1, 4, 'B', 3, "AAPL", 1000500), 100));
      feed(WithTs(MsgE(1, 4, 2), 200));
      fed = true;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(300));
  }
  const int status = ::pclose(child);
  CHECK(eof && WIFEXITED(status) && WEXITSTATUS(status) == 0);

  auto has = [&](const std::string &s) { return out.find(s) != std::string::npos; };
  CHECK(has("\"type\":\"ack\"") && has("\"status_text\":\"ok\""));
  CHECK(has("\"symbol\":\"AAPL\"") && has("\"snapshot\":true"));
  CHECK(has("\"bids\":[[1000000,10,1],[990000,20,1]]") && has("\"asks\":[[1010000,5,1]]"));       // the snapshot, price/shares/orders
  CHECK(fed && has("\"type\":\"trade\"") && has("\"kind\":\"E\"") && has("\"price\":1000500") && has("\"shares\":2"));
  CHECK(has("\"bids\":[[1000500,1,1],[1000000,10,1],[990000,20,1]]"));                           // after the new bid was hit for 2
  CHECK(!has("\"type\":\"gap\"") && !has("error:"));
  CHECK(books.interest(1) == 0);                                                                 // the client left: nothing is watching AAPL
  std::puts("ok");
  return 0;
#endif
}

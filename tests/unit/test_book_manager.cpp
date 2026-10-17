// Feeds hand-built ITCH 5.0 bytes (offsets taken from the spec tables) through the
// real decoder into BookManager.
#include <cstring>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include "check.h"
#include "itch_builders.h"
#include "itch/book/book_manager.hpp"

using namespace feed_handler;
using itchtest::Bytes;
using namespace itchtest;

static void feed(NasdaqItchDecoder &d, BookManager &m, const Bytes &b) {
  size_t used = 0;
  CHECK(d.DecodeMessage(reinterpret_cast<const char *>(b.data()), b.size(), used, m));
  CHECK(used == b.size());
}

int main() {
  spdlog::set_level(spdlog::level::off);
  NasdaqItchDecoder d; BookManager m; m.set_track_crossing(true);
  int32_t p; uint64_t s;

  feed(d, m, MsgR(1, "AAPL")); feed(d, m, MsgR(2, "MSFT"));
  CHECK(m.book_count() == 2 && std::string(m.symbol(1)) == "AAPL" && std::string(m.symbol(2)) == "MSFT");
  feed(d, m, MsgH(1, "AAPL", 'T'));
  CHECK(m.trading_state(1) == 'T');

  // A and F both add; books are separate per locate
  feed(d, m, MsgA(1, 10, 'B', 100, "AAPL", 1500000));
  feed(d, m, MsgF(1, 11, 'S', 200, "AAPL", 1500100));
  feed(d, m, MsgA(2, 12, 'B', 5, "MSFT", 3000000));
  CHECK(m.book(1)->best_bid(p, s) && p == 1500000 && s == 100);
  CHECK(m.book(1)->best_ask(p, s) && p == 1500100 && s == 200);
  CHECK(m.book(2)->best_bid(p, s) && p == 3000000 && s == 5);
  CHECK(m.live_orders() == 3);

  // E and C both execute; X partially cancels; D deletes
  feed(d, m, MsgE(1, 10, 40));
  CHECK(m.book(1)->best_bid(p, s) && s == 60);
  feed(d, m, MsgC(1, 11, 50, 1500050));                       // executes at a different price; book uses display price
  CHECK(m.book(1)->best_ask(p, s) && p == 1500100 && s == 150);
  feed(d, m, MsgX(1, 11, 100));
  CHECK(m.book(1)->best_ask(p, s) && s == 50);
  feed(d, m, MsgD(1, 11));
  CHECK(!m.book(1)->best_ask(p, s));

  // U replaces: new ref, new price, side carried over
  feed(d, m, MsgU(1, 10, 13, 25, 1500200));
  CHECK(m.book(1)->best_bid(p, s) && p == 1500200 && s == 25);
  CHECK(m.live_orders() == 2);

  // unknown locate / ref and invalid side never crash and are counted
  feed(d, m, MsgD(1, 9999));
  CHECK(m.aggregate_book_stats().unknown_ref == 1);
  feed(d, m, MsgA(3, 50, 'B', 1, "NEW", 100));               // no Stock Directory first
  CHECK(m.stats().lazy_books == 1 && m.book(3) != nullptr);
  Bytes bad = MsgA(1, 60, 'X', 1, "AAPL", 100);
  feed(d, m, bad);
  CHECK(m.stats().bad_side == 1 && m.live_orders() == 3);

  // crossing is counted, validate_all still passes
  feed(d, m, MsgA(1, 70, 'S', 10, "AAPL", 1500000));          // ask below best bid 150.0200
  CHECK(m.book(1)->crossed() && m.stats().crossing_events >= 1 && m.crossed_books() == 1);
  CHECK(m.validate_all());

  std::puts("ok");
  return 0;
}

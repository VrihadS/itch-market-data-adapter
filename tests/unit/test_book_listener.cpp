// BookManager -> BookListener: which events fire, in which order, and the "top N changed"
// tracking that decides L2 updates.
#include <algorithm>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>

#include "check.h"
#include "itch_builders.h"
#include "itch/book/book_manager.hpp"

using namespace feed_handler;
using namespace itchtest;

struct Recorder : BookListener {
  struct Ev { bool is_trade; TradeEvent t; uint16_t loc; uint64_t ts; };
  std::vector<Ev> ev;
  void on_book_update(uint16_t loc, const OrderBook &, uint64_t ts) override { ev.push_back({false, {}, loc, ts}); }
  void on_trade(const TradeEvent &t) override { ev.push_back({true, t, t.locate, t.exchange_ts_ns}); }
  size_t books() const { return std::count_if(ev.begin(), ev.end(), [](const Ev &e) { return !e.is_trade; }); }
  size_t trades() const { return ev.size() - books(); }
};

static void feed(NasdaqItchDecoder &d, BookManager &m, const Bytes &b) {
  size_t used = 0;
  CHECK(d.DecodeMessage(reinterpret_cast<const char *>(b.data()), b.size(), used, m));
}

// Soundness: whenever the visible top-N image changed, depth_touched() must have been true.
static void property_touch_is_sound() {
  for (uint32_t depth : {1u, 3u, 10u}) {
    std::mt19937_64 g(depth);
    OrderStore st; OrderBook b(st, 1);
    std::vector<uint64_t> live; uint64_t next = 1; size_t flagged = 0, changed = 0, unflagged_unchanged = 0;
    auto image = [&](uint8_t side) { std::vector<DepthLevel> v(depth); v.resize(b.depth(side, depth, v.data())); return v; };
    auto same = [](const std::vector<DepthLevel> &a, const std::vector<DepthLevel> &c) {
      if (a.size() != c.size()) return false;
      for (size_t i = 0; i < a.size(); ++i) if (a[i].price != c[i].price || a[i].shares != c[i].shares || a[i].orders != c[i].orders) return false;
      return true; };
    for (int i = 0; i < 200000; ++i) {
      const auto b0 = image(0), a0 = image(1);
      b.begin_touch(depth);
      const unsigned r = g() % 100;
      if (live.empty() || r < 45) {
        const uint8_t side = g() & 1; const int32_t px = 100000 + (side ? 1 : -1) * (int32_t)(g() % 40) * 100;
        b.on_add(next, px, 1 + g() % 50, side); live.push_back(next++);
      } else {
        const size_t k = g() % live.size(); const uint64_t ref = live[k];
        if (r < 70) { if (b.on_execute(ref, 1 + g() % 60).ok && st.find(ref) == NULL_IDX) { live[k] = live.back(); live.pop_back(); } }
        else if (r < 85) { b.on_cancel(ref, 1 + g() % 60); if (st.find(ref) == NULL_IDX) { live[k] = live.back(); live.pop_back(); } }
        else if (r < 93) { b.on_delete(ref); live[k] = live.back(); live.pop_back(); }
        else { const int32_t px = 100000 + (int32_t)(g() % 80 - 40) * 100; b.on_replace(ref, next, 1 + g() % 50, px); live[k] = live.back(); live.pop_back(); live.push_back(next++); }
      }
      const bool differs = !same(b0, image(0)) || !same(a0, image(1));
      if (differs) { ++changed; CHECK(b.depth_touched()); }
      else if (!b.depth_touched()) ++unflagged_unchanged;
      if (b.depth_touched()) ++flagged;
    }
    CHECK(b.validate());
    std::printf("  depth %2u: image changed %zu times, flagged %zu times (never missed one); %zu ops correctly left unflagged\n",
                depth, changed, flagged, unflagged_unchanged);
    CHECK(unflagged_unchanged > 0);   // tracking actually filters something
  }
}

int main() {
  spdlog::set_level(spdlog::level::off);
  property_touch_is_sound();

  NasdaqItchDecoder d; BookManager m; Recorder rec; m.set_listener(&rec);
  feed(d, m, MsgR(1, "AAPL")); feed(d, m, MsgR(2, "MSFT"));

  // symbol lookup ignores padding; unknown symbols are absent
  CHECK(m.locate_of("AAPL") && *m.locate_of("AAPL") == 1 && *m.locate_of("MSFT   ") == 2);
  CHECK(!m.locate_of("NOPE") && m.find_by_symbol("AAPL") == m.book(1));

  // nothing fires without interest
  feed(d, m, MsgA(1, 1, 'B', 100, "AAPL", 1000000));
  CHECK(rec.ev.empty());

  // L2 depth 2 + trades on AAPL only
  m.set_interest(1, kInterestBook | kInterestTrades, 2);
  feed(d, m, MsgA(1, 2, 'B', 50, "AAPL", 990000));            // rank 1 -> inside top 2
  CHECK(rec.books() == 1 && rec.ev.back().loc == 1);
  feed(d, m, MsgA(1, 3, 'B', 70, "AAPL", 980000));            // rank 2 -> outside top 2
  CHECK(rec.books() == 1);
  feed(d, m, MsgA(1, 4, 'S', 10, "AAPL", 1010000));           // first ask -> rank 0
  CHECK(rec.books() == 2);
  feed(d, m, MsgA(2, 5, 'B', 10, "MSFT", 3000000));           // MSFT has no interest
  CHECK(rec.books() == 2);
  feed(d, m, MsgX(1, 3, 20));                                 // shrink the order outside the window
  CHECK(rec.books() == 2);
  feed(d, m, MsgD(1, 2));                                     // removes level 99.00 -> 98.00 shifts in
  CHECK(rec.books() == 3);

  // E: trade reported first (display price, resting side), then the book change
  rec.ev.clear();
  feed(d, m, WithTs(MsgE(1, 1, 40), 123456));
  CHECK(rec.ev.size() == 2 && rec.ev[0].is_trade && !rec.ev[1].is_trade);
  CHECK(rec.ev[0].t.kind == 'E' && rec.ev[0].t.side == 'B' && rec.ev[0].t.price == 1000000 && rec.ev[0].t.shares == 40);
  CHECK(rec.ev[0].t.match_number == 77 && rec.ev[0].t.exchange_ts_ns == 123456 && rec.ev[1].ts == 123456);
  // executing more than rests clamps to what was resting
  rec.ev.clear();
  feed(d, m, MsgE(1, 1, 1000));
  CHECK(rec.trades() == 1 && rec.ev[0].t.shares == 60);

  // C: printable executions are prints at the explicit price; non-printable are not
  feed(d, m, MsgA(1, 6, 'S', 30, "AAPL", 1020000));
  rec.ev.clear();
  feed(d, m, MsgCx(1, 6, 10, 1015000, 'Y'));
  CHECK(rec.trades() == 1 && rec.ev[0].t.kind == 'C' && rec.ev[0].t.price == 1015000 && rec.ev[0].t.side == 'S');
  rec.ev.clear();
  feed(d, m, MsgCx(1, 6, 10, 1015000, 'N'));
  CHECK(rec.trades() == 0);

  // P, Q, B need no book; they follow trade interest only
  rec.ev.clear();
  feed(d, m, MsgP(1, 'S', 25, "AAPL", 1005000, 900));
  feed(d, m, MsgQ(1, 5000000000ull, "AAPL", 1006000, 901, 'C'));
  feed(d, m, MsgB(1, 900));
  CHECK(rec.trades() == 3 && rec.books() == 0);
  CHECK(rec.ev[0].t.kind == 'P' && rec.ev[0].t.side == 'S' && rec.ev[0].t.price == 1005000 && rec.ev[0].t.match_number == 900);
  CHECK(rec.ev[1].t.kind == 'Q' && rec.ev[1].t.cross_type == 'C' && rec.ev[1].t.shares == 5000000000ull && rec.ev[1].t.side == 0);
  CHECK(rec.ev[2].t.kind == 'B' && rec.ev[2].t.match_number == 900);
  feed(d, m, MsgP(2, 'B', 5, "MSFT", 1, 1));                  // other locate: silent
  CHECK(rec.trades() == 3);

  // trades-only interest: no book events; book-only: no trades
  m.set_interest(1, kInterestTrades, 0);
  rec.ev.clear();
  feed(d, m, MsgA(1, 7, 'B', 10, "AAPL", 995000)); feed(d, m, MsgE(1, 7, 5));
  CHECK(rec.books() == 0 && rec.trades() == 1);
  m.set_interest(1, kInterestBook, 5);
  rec.ev.clear();
  feed(d, m, MsgE(1, 7, 5));
  CHECK(rec.trades() == 0 && rec.books() == 1);

  // clearing interest silences everything and stops tracking
  m.set_interest(1, 0, 0);
  rec.ev.clear();
  feed(d, m, MsgA(1, 8, 'B', 10, "AAPL", 1000100)); feed(d, m, MsgP(1, 'B', 5, "AAPL", 1, 1));
  CHECK(rec.ev.empty() && m.validate_all());

  std::puts("ok");
  return 0;
}

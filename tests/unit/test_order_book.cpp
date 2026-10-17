#include "check.h"
#include "itch/book/order_book.hpp"

using namespace feed_handler;

static constexpr int32_t kP100 = 1000000;   // 100.0000

int main() {
  int32_t p; uint64_t s;

  { // best bid/ask ordering, independent of insertion order
    OrderStore st; OrderBook b(st, 1);
    b.on_add(1, kP100, 100, 0);
    b.on_add(2, kP100 - 100, 50, 0);                  // lower bid
    b.on_add(3, kP100 + 100, 70, 0);                  // higher bid
    CHECK(b.best_bid(p, s) && p == kP100 + 100 && s == 70);
    b.on_add(4, kP100 + 300, 10, 1);
    b.on_add(5, kP100 + 200, 20, 1);                  // lower ask
    CHECK(b.best_ask(p, s) && p == kP100 + 200 && s == 20);
    CHECK(!b.crossed() && b.validate());
  }
  { // far-apart levels (the old neighbour-scan bug): 5000 units away
    OrderStore st; OrderBook b(st, 1);
    b.on_add(1, kP100, 100, 0);
    b.on_add(2, kP100 - 5000, 100, 0);
    CHECK(b.best_bid(p, s) && p == kP100 && b.level_count() == 2 && b.validate());
  }
  { // partial execute keeps queue position; full execute removes order and level
    OrderStore st; OrderBook b(st, 1);
    b.on_add(1, kP100, 100, 0);
    b.on_add(2, kP100, 50, 0);
    b.on_execute(1, 30);
    CHECK(b.best_bid(p, s) && s == 120 && st.live_orders() == 2 && b.validate());
    b.on_execute(1, 70);                               // exactly the remainder
    CHECK(b.best_bid(p, s) && s == 50 && st.live_orders() == 1 && b.validate());
    b.on_execute(2, 50);
    CHECK(!b.best_bid(p, s) && st.live_orders() == 0 && b.level_count() == 0 && b.validate());
  }
  { // partial cancel, then cancel more than remains (clamped), then delete
    OrderStore st; OrderBook b(st, 1);
    b.on_add(1, kP100, 100, 1);
    b.on_cancel(1, 40);
    CHECK(b.best_ask(p, s) && s == 60);
    b.on_cancel(1, 1000);
    CHECK(!b.best_ask(p, s) && st.live_orders() == 0);
    b.on_add(2, kP100, 10, 1); b.on_delete(2);
    CHECK(!b.best_ask(p, s) && st.live_orders() == 0 && b.validate());
  }
  { // replace: new ref, new price/size, side kept, loses time priority
    OrderStore st; OrderBook b(st, 1);
    b.on_add(1, kP100, 100, 0);
    b.on_add(2, kP100, 100, 0);
    b.on_replace(1, 3, 100, kP100);                    // same price: goes behind order 2
    CHECK(st.find(1) == NULL_IDX && st.find(3) != NULL_IDX && b.validate());
    b.on_execute(2, 100);                              // order 2 was ahead, so order 3 remains
    CHECK(b.best_bid(p, s) && s == 100 && st.find(3) != NULL_IDX);
    b.on_replace(3, 4, 25, kP100 + 500);
    CHECK(b.best_bid(p, s) && p == kP100 + 500 && s == 25 && st.live_orders() == 1 && b.validate());
  }
  { // FIFO within a level: executions hit the oldest order first via the ref the feed names
    OrderStore st; OrderBook b(st, 1);
    for (uint64_t r = 1; r <= 5; ++r) b.on_add(r, kP100, 10, 0);
    b.on_delete(3);                                    // middle of the queue
    b.on_delete(1);                                    // head
    b.on_delete(5);                                    // tail
    CHECK(b.best_bid(p, s) && s == 20 && b.validate());
  }
  { // anomalies are counted, never crash
    OrderStore st; OrderBook b(st, 1);
    b.on_execute(99, 1); b.on_cancel(99, 1); b.on_delete(99); b.on_replace(99, 100, 1, kP100);
    CHECK(b.stats().unknown_ref == 4);
    CHECK(b.on_add(1, kP100, 10, 0));
    CHECK(!b.on_add(1, kP100, 10, 0));                 // duplicate live ref
    CHECK(b.stats().rejected_add == 1 && st.live_orders() == 1 && b.validate());
    CHECK(b.on_add(0xFFFFFFFFFFFFFFF0ull, kP100, 10, 0));   // full 64-bit refs are fine
    CHECK(st.find(0xFFFFFFFFFFFFFFF0ull) != NULL_IDX && b.validate());
    b.on_delete(0xFFFFFFFFFFFFFFF0ull);
    OrderBook other(st, 2);                            // order belongs to locate 1
    other.on_delete(1);
    CHECK(other.stats().wrong_locate == 1 && st.live_orders() == 1);
  }
  { // crossed is reported but does not fail validate()
    OrderStore st; OrderBook b(st, 1);
    b.on_add(1, kP100 + 100, 10, 0);
    b.on_add(2, kP100, 10, 1);
    CHECK(b.crossed() && b.validate());
  }
  std::puts("ok");
  return 0;
}

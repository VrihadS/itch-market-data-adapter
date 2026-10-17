// Random multi-symbol order flow checked against an independent model after every operation.
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <unordered_map>
#include <vector>

#include "check.h"
#include "itch/book/order_book.hpp"

using namespace feed_handler;

struct Ref { uint16_t loc; int32_t px; uint32_t sh; uint8_t side; };

int main(int argc, char **argv) {
  const uint64_t seeds = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 20;
  const int      kOps  = 100000;
  for (uint64_t seed = 1; seed <= seeds; ++seed) {
    std::mt19937_64 g(seed);
    OrderStore store;
    constexpr int kSyms = 5;
    std::vector<OrderBook> books;
    for (int i = 0; i < kSyms; ++i) books.emplace_back(store, static_cast<uint16_t>(i + 1));
    std::unordered_map<uint64_t, Ref> refs;
    std::map<int32_t, uint64_t> mb[kSyms], ma[kSyms];
    std::vector<uint64_t> live;                       // for O(1) random pick
    uint64_t next = 1;

    auto drop = [&](uint64_t r, uint32_t sh) {        // model: remove up to sh from ref r
      Ref &o = refs[r];
      const uint32_t f = std::min(sh, o.sh);
      auto &lv = (o.side ? ma : mb)[o.loc - 1];
      auto it = lv.find(o.px); it->second -= f; if (!it->second) lv.erase(it);
      o.sh -= f;
      return o.sh == 0;
    };
    auto erase_live = [&](size_t i) { live[i] = live.back(); live.pop_back(); };
    auto verify = [&](int s) {
      int32_t p; uint64_t q;
      bool h = books[s].best_bid(p, q);
      CHECK(h == !mb[s].empty());
      if (h) CHECK(p == mb[s].rbegin()->first && q == mb[s].rbegin()->second);
      h = books[s].best_ask(p, q);
      CHECK(h == !ma[s].empty());
      if (h) CHECK(p == ma[s].begin()->first && q == ma[s].begin()->second);
    };

    for (int i = 0; i < kOps; ++i) {
      const int s = g() % kSyms; const uint16_t loc = s + 1;
      const unsigned r = g() % 100;
      if (live.empty() || r < 45) {                   // add (near-touch and occasional far prices)
        const uint8_t side = g() & 1;
        const int32_t px = 1000000 + (int32_t)(g() % 60 - 30) * 100 + ((g() % 50 == 0) ? (int32_t)(g() % 20000) : 0);
        const uint32_t sh = 1 + g() % 500;
        CHECK(books[s].on_add(next, px, sh, side));
        refs[next] = {loc, px, sh, side}; (side ? ma : mb)[s][px] += sh; live.push_back(next); ++next;
        verify(s);
      } else {
        const size_t idx = g() % live.size(); const uint64_t ref = live[idx]; const Ref o = refs[ref];
        const int os = o.loc - 1;
        if (r < 70) {                                 // execute some or all
          const uint32_t sh = 1 + g() % (o.sh + 5);
          books[os].on_execute(ref, sh);
          if (drop(ref, sh)) { refs.erase(ref); erase_live(idx); }
        } else if (r < 82) {                          // partial/over cancel
          const uint32_t sh = 1 + g() % (o.sh + 5);
          books[os].on_cancel(ref, sh);
          if (drop(ref, sh)) { refs.erase(ref); erase_live(idx); }
        } else if (r < 92) {                          // delete
          books[os].on_delete(ref);
          drop(ref, UINT32_MAX); refs.erase(ref); erase_live(idx);
        } else {                                      // replace
          const int32_t px = 1000000 + (int32_t)(g() % 60 - 30) * 100;
          const uint32_t sh = 1 + g() % 500;
          books[os].on_replace(ref, next, sh, px);
          drop(ref, UINT32_MAX); refs.erase(ref); erase_live(idx);
          refs[next] = {o.loc, px, sh, o.side}; (o.side ? ma : mb)[os][px] += sh; live.push_back(next); ++next;
        }
        verify(os);
      }
      if (i % 5000 == 0) for (auto &b : books) CHECK(b.validate());
    }
    for (auto &b : books) { CHECK(b.validate()); CHECK(b.stats().unknown_ref == 0 && b.stats().inconsistent == 0); }
    CHECK(store.live_orders() == refs.size());
  }
  std::printf("ok (%llu seeds x %d ops)\n", (unsigned long long)seeds, kOps);
  return 0;
}

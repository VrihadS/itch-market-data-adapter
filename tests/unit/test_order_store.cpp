// OrderStore's ref index vs std::unordered_map under heavy churn, including colliding and
// full-64-bit refs (exercises growth and backward-shift deletion).
#include <cstdint>
#include <cstdio>
#include <random>
#include <unordered_map>
#include <vector>

#include "check.h"
#include "itch/book/order_store.h"

using namespace feed_handler;

int main() {
  for (uint64_t seed = 1; seed <= 5; ++seed) {
    std::mt19937_64 g(seed);
    OrderStore st;
    std::unordered_map<uint64_t, uint32_t> model;     // ref -> pool idx
    std::vector<uint64_t> live;
    uint64_t next = 1;
    for (int i = 0; i < 400000; ++i) {
      if (live.empty() || g() % 100 < 52) {
        // mix of sequential refs, huge refs, and refs with identical low bits
        const uint64_t ref = (g() % 3 == 0) ? (g() | (1ull << 63)) : (g() % 5 == 0 ? (next << 20) : next);
        ++next;
        const uint32_t idx = st.alloc(ref);
        if (model.count(ref)) { CHECK(idx == NULL_IDX); continue; }   // duplicate rejected
        CHECK(idx != NULL_IDX);
        model[ref] = idx; live.push_back(ref);
      } else {
        const size_t k = g() % live.size(); const uint64_t ref = live[k];
        CHECK(st.find(ref) == model[ref] && st.at(model[ref]).ref == ref);
        st.free(model[ref]); model.erase(ref);
        live[k] = live.back(); live.pop_back();
        CHECK(st.find(ref) == NULL_IDX);
      }
      CHECK(st.live_orders() == model.size());
      if (i % 20000 == 0) for (const auto &kv : model) CHECK(st.find(kv.first) == kv.second);
    }
    for (const auto &kv : model) CHECK(st.find(kv.first) == kv.second);
    CHECK(st.find(0xDEADBEEFDEADBEEFull) == NULL_IDX || model.count(0xDEADBEEFDEADBEEFull));
  }
  std::puts("ok");
  return 0;
}

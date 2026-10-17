// Pre-sized OrderStore: no heap allocation while within capacity; transparent, counted
// fallback to the heap beyond it.
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <random>
#include <unordered_map>
#include <vector>

#include "check.h"
#include "itch/book/order_store.h"

static unsigned long long g_allocs = 0;
void *operator new(size_t n) { ++g_allocs; void *p = std::malloc(n); if (!p) throw std::bad_alloc(); return p; }
void operator delete(void *p) noexcept { std::free(p); }
void operator delete(void *p, size_t) noexcept { std::free(p); }

using namespace feed_handler;

int main() {
  constexpr size_t kCap = 50000;
  std::mt19937_64 g(7);

  { // within capacity: zero allocations through heavy churn, and no fallbacks counted
    OrderStore st(kCap);
    std::vector<uint64_t> live; live.reserve(kCap);     // test's own storage, allocated before measuring
    std::vector<uint32_t> idx;  idx.reserve(kCap);
    uint64_t next = 1;
    const auto before = g_allocs;
    for (int i = 0; i < 600000; ++i) {
      if (live.size() < kCap && (live.empty() || g() % 100 < 55)) {
        const uint64_t ref = next++ * 0x10001ull;
        const uint32_t k = st.alloc(ref); CHECK(k != NULL_IDX);
        live.push_back(ref); idx.push_back(k);
      } else {
        const size_t j = g() % live.size();
        CHECK(st.find(live[j]) == idx[j]);
        st.free(idx[j]);
        live[j] = live.back(); live.pop_back(); idx[j] = idx.back(); idx.pop_back();
      }
    }
    CHECK(g_allocs == before);                           // the point of pre-sizing
    CHECK(st.stats().total() == 0);
  }
  { // beyond capacity: keeps working, counts the heap growth, results stay correct
    OrderStore st(1000);
    std::unordered_map<uint64_t, uint32_t> model;
    for (uint64_t r = 1; r <= 20000; ++r) { const uint32_t k = st.alloc(r); CHECK(k != NULL_IDX); model[r] = k; }
    CHECK(st.stats().pool_growths > 0 && st.stats().table_growths > 0);
    for (const auto &kv : model) CHECK(st.find(kv.first) == kv.second && st.at(kv.second).ref == kv.first);
    for (uint64_t r = 1; r <= 20000; r += 2) st.free(model[r]);
    for (uint64_t r = 2; r <= 20000; r += 2) CHECK(st.find(r) == model[r]);
    CHECK(st.live_orders() == 10000);
  }
  std::puts("ok");
  return 0;
}

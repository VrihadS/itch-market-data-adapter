// SPSC ring: ordering, wrap-around, full detection, control reserve, and a two-thread stress.
#include <atomic>
#include <cstdint>
#include <cstring>
#include <random>
#include <thread>
#include <vector>

#include "check.h"
#include "itch/feed/spsc_ring.hpp"

using feed_handler::feed::SpscRing;

static uint32_t checksum(const uint8_t *p, size_t n) { uint32_t h = 2166136261u; for (size_t i = 0; i < n; ++i) h = (h ^ p[i]) * 16777619u; return h; }

int main() {
  { // basics
    SpscRing r(4096, 256);
    SpscRing::Record rec; CHECK(!r.peek(rec));
    const char a[] = "hello"; CHECK(r.push(7, a, 5)); CHECK(r.push(8, nullptr, 0));
    CHECK(r.peek(rec) && rec.type == 7 && rec.size == 5 && std::memcmp(rec.data, "hello", 5) == 0); r.pop();
    CHECK(r.peek(rec) && rec.type == 8 && rec.size == 0); r.pop();
    CHECK(!r.peek(rec) && r.used_bytes() == 0);
    CHECK(r.claim(1, (uint32_t)r.max_payload() + 1) == nullptr);                       // too large for the ring
  }
  { // wrap-around with random sizes against a model queue, ring never more than ~full
    SpscRing r(4096, 0); std::mt19937 g(5); std::vector<std::vector<uint8_t>> model; size_t head = 0, wraps = 0, fulls = 0;
    for (int i = 0; i < 200000; ++i) {
      if (g() % 2) {
        std::vector<uint8_t> v(g() % 300); for (auto &b : v) b = (uint8_t)g();
        if (r.push(i, v.data(), (uint32_t)v.size())) model.push_back(v); else ++fulls;
      } else if (head < model.size()) {
        SpscRing::Record rec; CHECK(r.peek(rec)); CHECK(rec.size == model[head].size() && (rec.size == 0 || std::memcmp(rec.data, model[head].data(), rec.size) == 0));
        r.pop(); ++head;
      }
      if (r.used_bytes() > 3000) ++wraps;
    }
    SpscRing::Record rec; while (head < model.size()) { CHECK(r.peek(rec) && rec.size == model[head].size()); r.pop(); ++head; }
    CHECK(!r.peek(rec) && fulls > 0 && wraps > 0);
    std::printf("  model run: %zu records, ring filled up %zu times\n", model.size(), fulls); }
  { // data stops before the control reserve; control records still fit; draining frees space again
    SpscRing r(4096, 1024); const std::vector<uint8_t> blob(100, 1); size_t data = 0;
    while (r.push(1, blob.data(), 100)) ++data;
    CHECK(data > 5 && !r.push(1, blob.data(), 100));
    CHECK(r.push(2, blob.data(), 100, /*control=*/true));
    size_t ctl = 1; while (r.push(2, blob.data(), 100, true)) ++ctl; CHECK(ctl >= 5);
    SpscRing::Record rec; size_t n = 0; while (r.peek(rec)) { r.pop(); ++n; } CHECK(n == data + ctl);
    CHECK(r.push(1, blob.data(), 100)); }
  { // two threads: every record arrives once, in order, intact
    SpscRing r(1 << 16, 1024); constexpr uint64_t kN = 3000000; std::atomic<bool> bad{false}; uint64_t seen = 0;
    std::thread cons([&] {
      SpscRing::Record rec; uint64_t expect = 0;
      while (expect < kN) {
        if (!r.peek(rec)) { std::this_thread::yield(); continue; }
        uint64_t seq; uint32_t sum; std::memcpy(&seq, rec.data, 8); std::memcpy(&sum, rec.data + 8, 4);
        if (seq != expect || sum != checksum(rec.data + 12, rec.size - 12)) { bad = true; break; }
        r.pop(); ++expect;
      }
      seen = expect; });
    std::mt19937 g(9); uint8_t buf[200]; uint64_t stalls = 0;
    for (uint64_t i = 0; i < kN; ++i) {
      const uint32_t body = g() % 150; for (uint32_t k = 0; k < body; ++k) buf[12 + k] = (uint8_t)g();
      std::memcpy(buf, &i, 8); const uint32_t sum = checksum(buf + 12, body); std::memcpy(buf + 8, &sum, 4);
      while (!r.push(1, buf, 12 + body)) { ++stalls; if (bad) break; std::this_thread::yield(); }
    }
    cons.join(); CHECK(!bad && seen == kN);
    std::printf("  two threads: %llu records in order and intact (%llu producer stalls)\n", (unsigned long long)kN, (unsigned long long)stalls); }
  std::puts("ok");
  return 0;
}

#pragma once
// Random but valid ITCH order flow (as raw messages) for end-to-end feed tests.
#include <random>
#include <vector>

#include "itch_builders.h"

namespace flow {

using itchtest::Bytes;

inline std::vector<Bytes> make(size_t count, uint64_t seed, int symbols = 20) {
  using namespace itchtest;
  std::mt19937_64 g(seed);
  std::vector<Bytes> out;
  char name[16];
  for (int s = 1; s <= symbols; ++s) { std::snprintf(name, sizeof name, "SYM%02d", s); out.push_back(MsgR((uint16_t)s, name)); }
  struct Live { uint64_t ref; uint16_t loc; uint32_t shares; };
  std::vector<Live> live; uint64_t next_ref = 1, ts = 1000;
  while (out.size() < count) {
    ts += 1 + g() % 50;
    const unsigned r = g() % 100;
    if (live.empty() || r < 42) {
      const uint16_t loc = 1 + g() % symbols; const char side = (g() & 1) ? 'B' : 'S';
      const uint32_t px = 1000000 + (uint32_t)(g() % 40) * 100 + (side == 'S' ? 4100 : 0), sh = 1 + g() % 500;
      std::snprintf(name, sizeof name, "SYM%02d", loc);
      out.push_back(WithTs((g() % 5 == 0) ? MsgF(loc, next_ref, side, sh, name, px) : MsgA(loc, next_ref, side, sh, name, px), ts));
      live.push_back({next_ref++, loc, sh});
    } else {
      const size_t i = g() % live.size(); Live &o = live[i];
      if (r < 66) { const uint32_t q = 1 + g() % o.shares; out.push_back(WithTs(MsgE(o.loc, o.ref, q), ts)); o.shares -= q; if (!o.shares) { o = live.back(); live.pop_back(); } }
      else if (r < 76) { const uint32_t q = 1 + g() % o.shares; out.push_back(WithTs(MsgX(o.loc, o.ref, q), ts)); o.shares -= q; if (!o.shares) { o = live.back(); live.pop_back(); } }
      else if (r < 90) { out.push_back(WithTs(MsgD(o.loc, o.ref), ts)); o = live.back(); live.pop_back(); }
      else if (r < 95) { const uint32_t sh = 1 + g() % 400, px = 1000000 + (uint32_t)(g() % 40) * 100; out.push_back(WithTs(MsgU(o.loc, o.ref, next_ref, sh, px), ts)); o = {next_ref++, o.loc, sh}; }
      else { out.push_back(WithTs(MsgP(o.loc, 'B', 1 + g() % 50, "SYM01", 1000000, next_ref), ts)); }
    }
  }
  return out;
}

}  // namespace flow

#pragma once
// Hand-built ITCH 5.0 messages (offsets from the spec tables) for tests.
#include <cstdint>
#include <cstring>
#include <vector>

#include "itch/protocol/ndaq_itch_decoder.h"

namespace itchtest {

using Bytes = std::vector<unsigned char>;

inline void be(Bytes &b, size_t off, uint64_t v, int n) { for (int i = 0; i < n; ++i) b[off + i] = (v >> (8 * (n - 1 - i))) & 0xFF; }
inline Bytes hdr(char type, size_t size, uint16_t locate, uint64_t ts) { Bytes b(size, 0); b[0] = type; be(b, 1, locate, 2); be(b, 5, ts, 6); return b; }
inline void alpha(Bytes &b, size_t off, const char *s, size_t n) { std::memset(&b[off], ' ', n); std::memcpy(&b[off], s, std::strlen(s)); }

inline Bytes MsgR(uint16_t loc, const char *sym) { Bytes b = hdr('R', 39, loc, 1); alpha(b, 11, sym, 8); b[19] = 'Q'; be(b, 21, 100, 4); return b; }
inline Bytes MsgH(uint16_t loc, const char *sym, char state) { Bytes b = hdr('H', 25, loc, 1); alpha(b, 11, sym, 8); b[19] = state; return b; }
inline Bytes MsgA(uint16_t loc, uint64_t ref, char side, uint32_t sh, const char *sym, uint32_t px) {
  Bytes b = hdr('A', 36, loc, 2); be(b, 11, ref, 8); b[19] = side; be(b, 20, sh, 4); alpha(b, 24, sym, 8); be(b, 32, px, 4); return b; }
inline Bytes MsgF(uint16_t loc, uint64_t ref, char side, uint32_t sh, const char *sym, uint32_t px) {
  Bytes b = hdr('F', 40, loc, 2); be(b, 11, ref, 8); b[19] = side; be(b, 20, sh, 4); alpha(b, 24, sym, 8); be(b, 32, px, 4); alpha(b, 36, "GSCO", 4); return b; }
inline Bytes MsgE(uint16_t loc, uint64_t ref, uint32_t sh) { Bytes b = hdr('E', 31, loc, 3); be(b, 11, ref, 8); be(b, 19, sh, 4); be(b, 23, 77, 8); return b; }
inline Bytes MsgC(uint16_t loc, uint64_t ref, uint32_t sh, uint32_t px) {
  Bytes b = hdr('C', 36, loc, 3); be(b, 11, ref, 8); be(b, 19, sh, 4); be(b, 23, 78, 8); b[31] = 'Y'; be(b, 32, px, 4); return b; }
inline Bytes MsgX(uint16_t loc, uint64_t ref, uint32_t sh) { Bytes b = hdr('X', 23, loc, 3); be(b, 11, ref, 8); be(b, 19, sh, 4); return b; }
inline Bytes MsgD(uint16_t loc, uint64_t ref) { Bytes b = hdr('D', 19, loc, 3); be(b, 11, ref, 8); return b; }
inline Bytes MsgU(uint16_t loc, uint64_t oref, uint64_t nref, uint32_t sh, uint32_t px) {
  Bytes b = hdr('U', 35, loc, 3); be(b, 11, oref, 8); be(b, 19, nref, 8); be(b, 27, sh, 4); be(b, 31, px, 4); return b; }

inline Bytes MsgCx(uint16_t loc, uint64_t ref, uint32_t sh, uint32_t px, char printable) {
  Bytes b = MsgC(loc, ref, sh, px); b[31] = printable; return b; }
inline Bytes MsgP(uint16_t loc, char side, uint32_t sh, const char *sym, uint32_t px, uint64_t match) {
  Bytes b = hdr('P', 44, loc, 4); be(b, 11, 0, 8); b[19] = side; be(b, 20, sh, 4); alpha(b, 24, sym, 8); be(b, 32, px, 4); be(b, 36, match, 8); return b; }
inline Bytes MsgQ(uint16_t loc, uint64_t sh, const char *sym, uint32_t px, uint64_t match, char cross) {
  Bytes b = hdr('Q', 40, loc, 4); be(b, 11, sh, 8); alpha(b, 19, sym, 8); be(b, 27, px, 4); be(b, 31, match, 8); b[39] = cross; return b; }
inline Bytes MsgB(uint16_t loc, uint64_t match) { Bytes b = hdr('B', 19, loc, 4); be(b, 11, match, 8); return b; }
// Overwrite the 6-byte timestamp (offset 5) of any message.
inline Bytes WithTs(Bytes b, uint64_t ts) { be(b, 5, ts, 6); return b; }

} // namespace itchtest

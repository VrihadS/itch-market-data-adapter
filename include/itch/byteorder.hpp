#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace itch {

// ITCH 5.0 is big-endian on the wire. These helpers read unaligned fields safely.

inline uint16_t read_be16(const void* p) noexcept {
  uint8_t b[2];
  std::memcpy(b, p, 2);
  return static_cast<uint16_t>((b[0] << 8) | b[1]);
}

inline uint32_t read_be32(const void* p) noexcept {
  uint8_t b[4];
  std::memcpy(b, p, 4);
  return (uint32_t{b[0]} << 24) | (uint32_t{b[1]} << 16) | (uint32_t{b[2]} << 8) | b[3];
}

inline uint64_t read_be64(const void* p) noexcept {
  uint8_t b[8];
  std::memcpy(b, p, 8);
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | b[i];
  return v;
}

// ITCH timestamps are 6 bytes: nanoseconds since midnight.
inline uint64_t read_be48(const void* p) noexcept {
  uint8_t b[6];
  std::memcpy(b, p, 6);
  uint64_t v = 0;
  for (int i = 0; i < 6; ++i) v = (v << 8) | b[i];
  return v;
}

inline void write_be16(void* p, uint16_t v) noexcept {
  uint8_t b[2] = {static_cast<uint8_t>(v >> 8), static_cast<uint8_t>(v)};
  std::memcpy(p, b, 2);
}

inline void write_be64(void* p, uint64_t v) noexcept {
  uint8_t b[8];
  for (int i = 7; i >= 0; --i) { b[i] = static_cast<uint8_t>(v); v >>= 8; }
  std::memcpy(p, b, 8);
}

}  // namespace itch

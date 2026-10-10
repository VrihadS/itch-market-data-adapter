#include <cstdio>
#include <cstdlib>

#include "itch/byteorder.hpp"

#define CHECK(cond)                                                   \
  do {                                                                \
    if (!(cond)) {                                                    \
      std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
      std::exit(1);                                                   \
    }                                                                 \
  } while (0)

int main() {
  const unsigned char buf[8] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
  CHECK(itch::read_be16(buf) == 0x0102);
  CHECK(itch::read_be32(buf) == 0x01020304);
  CHECK(itch::read_be48(buf) == 0x010203040506ULL);
  CHECK(itch::read_be64(buf) == 0x0102030405060708ULL);
  // Unaligned read
  CHECK(itch::read_be16(buf + 1) == 0x0203);
  std::puts("ok");
  return 0;
}

#pragma once
#include <cstdio>
#include <cstdlib>

#define CHECK(cond)                                                              \
  do {                                                                           \
    if (!(cond)) {                                                               \
      std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);       \
      std::exit(1);                                                              \
    }                                                                            \
  } while (0)

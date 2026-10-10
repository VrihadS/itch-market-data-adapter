# ITCH Market Data Adapter

End-to-end Nasdaq TotalView-ITCH 5.0 market data adapter in C++20:
wire bytes -> MoldUDP64 framing -> ITCH parser -> order book -> normalized events.

> Status: scaffolding. Tested against historical sample data and a simulated
> MoldUDP64 feed only; not connected to a live exchange session.

## Layout

| Path | Purpose |
|---|---|
| `include/itch/` | Header-only library: byte order, messages, parser, book |
| `src/` | Non-header implementation as it grows |
| `tests/` | Unit, golden (spec-derived), and property tests |
| `tools/` | Replay tool, MoldUDP64 sender/fault injector, generator |
| `scripts/` | Data fetch and helper scripts |
| `docs/` | Design notes and benchmark write-ups |
| `data/` | Sample files (git-ignored) |

## Build and test

    cmake -S . -B build
    cmake --build build
    ctest --test-dir build --output-on-failure

Sanitizers: `cmake -S . -B build-asan -DITCH_SANITIZE=ON`

## Sample data

    scripts/fetch_sample.sh <file>.NASDAQ_ITCH50.gz

## Roadmap

1. Parser for all ITCH 5.0 message types, golden tests from the spec
2. Order book with differential tests against a reference model
3. MoldUDP64 receiver, gap detection, A/B arbitration, fault injector
4. SPSC output queue and a consumer demo
5. Profiling, benchmarks, fuzzing

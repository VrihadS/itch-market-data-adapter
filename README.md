# ITCH Market Data Adapter

End-to-end Nasdaq TotalView-ITCH 5.0 market data adapter in C++20:
wire bytes -> MoldUDP64 framing -> ITCH parser -> order book -> normalized events.

> Status: decoder, order books, the MoldUDP64 feed path (sequencing, A/B arbitration, gap recovery,
> mid-day catch-up) and the Aeron messaging layer are built and tested against a full historical ITCH day
> and a faulty simulated feed on loopback. Nothing here has been connected to a live exchange session.

## Layout

| Path | Purpose |
|---|---|
| `include/itch/` | Library headers: byte order, protocol decoder, order book, messaging |
| `src/` | Implementation: `net/`, `protocol/`, `book/`, `messaging/` |
| `tests/` | Unit, golden (spec-derived), and property tests |
| `tools/` | Replay tools, market data server/client/gateway |
| `clients/python/` | Pure-Python client (Windows / macOS / Linux) |
| `scripts/` | Data fetch and helper scripts |
| `docs/` | Design notes and benchmark write-ups |
| `data/` | Sample files (git-ignored) |

## Build and test

    cmake -S . -B build
    cmake --build build
    ctest --test-dir build --output-on-failure

Sanitizers: `cmake -S . -B build-asan -DITCH_SANITIZE=ON`

## The live feed path

`itch_feed_handler` receives MoldUDP64 on two redundant lines, arbitrates between them, recovers lost packets from
a re-request server, catches up from sequence 1 when it joins mid-day, and builds the order books;
`itch_mold_sender` replays a real ITCH file as MoldUDP64 with injectable faults to exercise it. See
[docs/feed.md](docs/feed.md).

## Distributing books to strategy servers

`include/itch/messaging/` serves the order books over Aeron (IPC on the same machine, UDP unicast or
multicast elsewhere). Clients subscribe per instrument to L2 depth (up to 10) and/or time & sales.
Clients without Aeron (Python, Windows) connect through a TCP gateway; see
[clients/python/README.md](clients/python/README.md). Details: [docs/messaging.md](docs/messaging.md).

## Sample data

    scripts/fetch_sample.sh <file>.NASDAQ_ITCH50.gz

## Roadmap

1. Parser for all ITCH 5.0 message types, golden tests from the spec
2. Order book with differential tests against a reference model
3. MoldUDP64 receiver, gap detection, A/B arbitration, fault injector
4. SPSC output queue and a consumer demo
5. Profiling, benchmarks, fuzzing

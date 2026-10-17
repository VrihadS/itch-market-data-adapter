# Messaging: distributing books and prints to strategy servers

`include/itch/messaging/` and `src/messaging/` turn the order books into a service. Clients ask
for instruments, and for what they want from each one, and the server streams exactly that.

```
 ITCH feed ─► decoder ─► BookManager ─► MarketDataServer ──Aeron IPC──────────► client (same machine)
                              │               ▲    │      └─Aeron UDP unicast──► client (other machine)
              interest flags ◄┘               │    └───────Aeron UDP multicast─► clients
                                   SubscribeRequest (control stream)
```

## What a client can ask for

Per instrument, one request carries up to 32 symbols and a set of features:

| Feature | Meaning |
|---|---|
| `kFeatureL2Book` | the best N price levels per side, N = 1..10, as a full image on every change |
| `kFeatureTimeAndSales` | every print: executions (`E`,`C`), non-displayed trades (`P`), crosses (`Q`), broken trades (`B`) |

A client subscribes, re-subscribes (replaces the earlier request for those symbols and returns a
fresh L2 snapshot), unsubscribes some features, or disconnects (`UnsubscribeRequest` with no symbols).

## Transports and who uses which

| Client location | Transport | Notes |
|---|---|---|
| Same machine | `aeron:ipc` | shared memory through the local media driver; client and server must use the same Aeron directory |
| Another machine | `aeron:udp` unicast | each machine runs its own media driver; the client names its own address and port |
| Many machines | `aeron:udp` multicast | clients naming the same group and stream share ONE publication on the server |

Control (requests) goes to the server on `aeron:ipc` stream 1001 and/or `aeron:udp?endpoint=<server>:40100`
stream 1001. Replies and data go to the **reply channel the client names in its request**, which the server
validates (see Security).

## Session

1. The client opens its reply subscription, then the control publication, and waits for the server.
2. `SubscribeRequest` -> the server resolves symbols (via the ITCH Stock Directory), records the
   subscription, opens a publication to the reply channel (asynchronously), and, once the client is
   attached, sends a `SubscribeAck` followed by an L2 snapshot for each L2 symbol.
3. Updates stream until the client unsubscribes, disconnects, or stops heartbeating (10 s).
4. The server sends a heartbeat every second; a client that hears nothing for 5 s reports a timeout.

Sequence numbers are per (reply channel, instrument, stream kind) and count every message the server
attempted, so a dropped message shows up as a gap at the client. A snapshot re-bases the L2 sequence.

## Wire protocol (`protocol.hpp`)

Little-endian, packed, every message starts with `Header{length, version, type}`. Decoding is strict:
wrong length, version, type, counts, or characters are rejected before any field is used.

| Message | Direction | Size |
|---|---|---|
| `SubscribeRequest` | client -> server | 376 B |
| `UnsubscribeRequest` | client -> server | 276 B |
| `Heartbeat` | both | 24 B |
| `SubscribeAck` | server -> client | 148 B |
| `L2Book` (+ levels) | server -> client | 36 B + 16 B per level (max 356 B) |
| `Trade` | server -> client | 60 B |

Prices are `int32` with four implied decimals; shares are `uint64`; timestamps are the ITCH time (ns
since midnight) plus the server's wall-clock send time (ns since epoch).

## Threading and back-pressure

The server runs on the **feed thread**: the `BookListener` callbacks and `poll_control()` must be called
from the thread that applies ITCH messages. It never blocks that thread. Publications are created
asynchronously; if a client's channel is full the message is dropped and counted
(`dropped_back_pressure`), never waited on. The next L2 update supersedes a dropped one; a dropped trade
is visible to the client as a sequence gap.

For instruments nobody subscribed to, the book costs one byte lookup per message.

## Running it

```
# server: replay the day to 10:00 ET at full speed, then 200k msgs/s; serves IPC and UDP port 40100
gzip -dc data/12302019.NASDAQ_ITCH50.gz | build/itch_md_server --preload-until 10:00:00 --rate 200000

# client on the same machine: IPC
build/itch_md_client --symbols AAPL,MSFT --l2 10 --tns

# client on another machine: UDP unicast (reply-host = its own address as the server sees it)
build/itch_md_client --server 10.0.0.1 --reply-host 10.0.0.7 --symbols AAPL --l2 5

# multicast
build/itch_md_client --server 10.0.0.1 --reply-host 10.0.0.7 --mcast 239.1.1.1 --symbols AAPL --tns
```

Without `--aeron-dir` the server starts an **embedded** media driver in Aeron's default directory. For
anything real, run the standalone `aeronmd` (`cmake --build build --target aeronmd`, produced at `build/_deps/aeron-build/binaries/aeronmd`)
on its own isolated, pinned cores and pass its directory with `--aeron-dir`.

Build options: `-DITCH_WITH_AERON=OFF` builds everything except the Aeron transport (the in-process
transport still runs all logic tests); `-DITCH_AERON_SOURCE_DIR=/path` uses a local Aeron checkout instead
of downloading tag 1.53.3.

## Clients without Aeron: TCP gateway and the Python client

Aeron has no official Python client, and Windows machines would each need a media driver. So
`itch_md_gateway` accepts plain TCP connections and speaks the **same binary protocol** over them,
framed by the message header's length. Each TCP connection gets its own upstream client, so
subscriptions, snapshots, sequence numbers and the L2/trade semantics are identical to a native client.

    Python / Windows --TCP--> itch_md_gateway --Aeron IPC or UDP--> itch_md_server

```
# on the server machine: the server, then the gateway (IPC to the server on the same machine)
gzip -dc data/12302019.NASDAQ_ITCH50.gz | build/itch_md_server --preload-until 10:00:00 --rate 100000 &
build/itch_md_gateway --listen 0.0.0.0 --port 40300 --allow 192.168.1.0/24

# on the Windows machine
py itch_md_client.py --host <server ip> --port 40300 --symbols AAPL,MSFT --l2 5 --tns
```

Details and library usage are in [clients/python/README.md](../clients/python/README.md). The gateway can
also run on a different machine from the server (`--server <ip> --reply-host <this ip>`, Aeron UDP).

- The gateway **ignores the reply channel** in a TCP client's request. Where data goes is decided by the
  gateway, never by the peer, so a TCP client cannot aim the feed anywhere.
- It never blocks on a slow peer: each connection has a bounded output queue (8 MB) and is closed when it
  overflows. It also closes peers that send malformed or server-to-client messages, that go silent for
  30 s, or when the market data server stops responding (the peer reconnects).
- **No authentication.** It listens on 127.0.0.1 by default. Use `--allow`, a private network, a VPN or an
  SSH tunnel before exposing it.
- Connecting a new peer to the market data server is a short blocking step (0.5 s timeout); with the server
  down, a new connection stalls the gateway for that long. Gateway code is POSIX only; the client is not.

## Shared multicast streams

Clients that name the same group, port **and stream id** (`--reply-stream`) share one server
publication. The stream carries the union of what the members asked for; every client keeps its own
subscriptions and ignores the rest (`ignored (not subscribed)` in the client summary). Clients with
different stream ids on the same group do not share and receive only their own data.

## Testing it yourself

**Automated** (about 6 s): `ctest --test-dir build --output-on-failure -R "messaging|market_data|listener"`.
`test_market_data_aeron` runs the real stack and prints `multicast: ok` or `multicast: SKIPPED`.

**Manual, one machine, several terminals.** Start the server (this replays the day to 10:00 ET at full
speed, then 100k msgs/s; `--mcast-interface 127.0.0.1` lets multicast work over loopback):

    gzip -dc data/12302019.NASDAQ_ITCH50.gz | build/itch_md_server --preload-until 10:00:00 \
        --rate 100000 --stop-at 10:30:00 --mcast-interface 127.0.0.1

Then, each in its own terminal:

    # IPC (same machine)
    build/itch_md_client --symbols AAPL,MSFT --l2 5 --tns
    # UDP unicast, over loopback standing in for another machine
    build/itch_md_client --server 127.0.0.1 --reply-host 127.0.0.1 --reply-port 40555 --symbols AAPL --l2 10
    # multicast: run these two together; same group, port and stream => one shared publication
    build/itch_md_client --server 127.0.0.1 --reply-host 127.0.0.1 --reply-port 40556 --reply-stream 777 \
        --mcast 239.30.10.77 --symbols AAPL --tns
    build/itch_md_client --server 127.0.0.1 --reply-host 127.0.0.1 --reply-port 40556 --reply-stream 777 \
        --mcast 239.30.10.77 --symbols MSFT --l2 3 --tns

What to look for: `ack ... ok` with the right locate per symbol, an L2 `SNAPSHOT` first, `gaps 0` in the
summary, and `drops(bp=0 nc=0)` on the server line. Add `--quiet` for counts only.

**Things to break on purpose:** `--l2 11` (bad depth), an unknown symbol (`unknown symbol` result),
Ctrl-C a client (the server drops it after 10 s of silence), stop the server (clients print `server
stopped responding` after 5 s), run two clients with the same `--reply-port` and no `--reply-stream`
(they get distinct random streams, so both work).

**Two machines.** Run the server with `--udp-port 40100` and `--allow <client network>`; open UDP 40100
and each client's `--reply-port` in any firewall. On the client machine run
`itch_md_client --server <server ip> --reply-host <this machine's ip> --symbols ...`. Each machine needs
its own media driver: the client tool starts none, so run `aeronmd` there first (or use the server's
embedded-driver pattern in your own client). For multicast, also set `--mcast-interface <server NIC ip>`
on the server and make sure the switches forward the group (IGMP) and the TTL allows it.

**Loss and reordering.** Loopback never loses packets, so it proves the logic, not Aeron's recovery. On
Linux, `tc qdisc add dev lo root netem loss 5%` (and `delay`, `reorder`) injects faults; watch `gaps` in
the client summary and the server's `drops`. This repo has not been run under fault injection.
macOS has no `netem`; `dnctl`/`pfctl` can do similar work.

## Security

- The reply channel is client-supplied, so it is parsed strictly: only `aeron:ipc` or
  `aeron:udp?endpoint=<IPv4>:<port>` are accepted. Extra Aeron parameters, hostnames, whitespace, port 0,
  and reserved ranges are rejected, so a client cannot set term lengths, control endpoints, and so on.
- **`--allow CIDR` restricts where UDP data may be sent. With no allow-list, anyone who can reach the
  control port can ask the server to send the feed to any address.** Set it in any shared network.
- An IPC or unicast reply channel belongs to the client that first opened it; another client asking for the
  same one is dropped without an answer (so nothing leaks to the owner).
- There is **no authentication or entitlement check** on the control channel. Restrict it with network
  controls. Redistributing exchange data typically also needs a licence; that is outside this code.

## Known limits

- L2 updates are full top-N images, not deltas, and are not conflated. At high rates, subscribe to fewer
  instruments or add conflation.
- No recovery of lost trades: a gap is reported, not repaired. A re-subscribe repairs the book.
- IPv4 only. Symbols are at most 8 characters (the ITCH field).
- Tested over loopback only: IPC, UDP unicast and multicast on one macOS host. Multicast across real
  networks (IGMP, TTL, switch configuration), cloud networks, and Linux kernel-bypass are untested.
- Latency has not been benchmarked under controlled conditions. The client tool prints a send-to-read time
  that includes its own polling sleep and is meaningful only on one machine.

## Tests

| Test | What it proves |
|---|---|
| `test_messaging_protocol` | round trips, rejection of malformed input, 300,000-input fuzz pass, channel validator incl. injection attempts |
| `test_book_listener` | which events the book raises and in what order; the "top N changed" flag never misses a real change (200,000 random operations at depths 1, 3, 10) |
| `test_market_data_inproc` | the full server/client logic over a deterministic in-process transport: ack-then-snapshot, depth filtering, prints before book changes, shared multicast sinks, channel ownership, allow-list, timeouts, back-pressure and gap detection, silent-server detection |
| `test_market_data_aeron` | the same stack over real Aeron: embedded driver, IPC, UDP unicast, multicast |
| `test_tcp_gateway` | the gateway over real loopback TCP: framing, id mapping, ignoring the peer's reply channel, malformed/hostile input, allow-list, connection limit, slow-peer cut-off, server unavailable, idle and upstream timeouts |
| `python_client` | the Python client: shared wire vectors (byte-identical with C++), stream reassembly, gap detection, error handling |
| `test_gateway_python` | the whole chain with the real Python client as a subprocess over Aeron |

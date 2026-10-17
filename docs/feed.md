# Feed handling: MoldUDP64 in, order books out

This is the part that turns the decoder and the books into a live feed handler: it receives the exchange's
MoldUDP64 stream on two redundant lines, repairs loss from a re-request server, survives late starts and
session changes, and hands an exact, in-order message stream to the book thread.

```
 line A (multicast) --\
 line B (multicast) ---+--> FeedSession --> RingSink --> SpscRing --> FeedConsumer --> BookManager --> MarketDataServer
 re-request replies --/       |  ^                                   (book thread)
 (unicast)                    |  +-- 1 ms tick                       decode + books + Aeron
                              +-- Request packets --> re-request server
          network thread (SocketManager, Boost.Asio) ---------------^   ^--------- book thread
```

| Piece | File | Role |
|---|---|---|
| Packet layer | [net/moldudp64.hpp](../include/itch/net/moldudp64.hpp) | Parse/build MoldUDP64 packets per the spec; a packet is validated as a whole before any block is exposed |
| Sockets | [net/socket_manager.h](../include/itch/net/socket_manager.h), [udp_receiver.h](../include/itch/net/udp_receiver.h), [udp_socket.h](../include/itch/net/udp_socket.h), [unicast_udp_socket.h](../include/itch/net/unicast_udp_socket.h) | Network thread, multicast/unicast receive, re-request send |
| Session logic | [feed/feed_session.hpp](../include/itch/feed/feed_session.hpp) | Sequencing, A/B arbitration, recovery, session lifecycle (no sockets, threads or clocks inside) |
| Hand-off | [feed/spsc_ring.hpp](../include/itch/feed/spsc_ring.hpp), [feed_records.hpp](../include/itch/feed/feed_records.hpp) | Wait-free ring between the two threads; control records get reserved space so they cannot be lost behind data |
| Wiring | [feed/feed_handler.hpp](../include/itch/feed/feed_handler.hpp), [feed_consumer.hpp](../include/itch/feed/feed_consumer.hpp) | Network side / book side |
| Test sender | [feed/mold_sender.hpp](../include/itch/feed/mold_sender.hpp) | Packetizer, fault injector, re-request server, paced two-line sender |

## What `FeedSession` guarantees

The sink sees every message of the session exactly once, in sequence order, or the session says it cannot.

- **A/B arbitration.** Both lines carry the same sequence numbers. Whichever delivers a message first wins; the
  other copy is dropped as a duplicate. A gap on one line is normally filled by the other with no request at all.
- **Gap recovery.** A gap that the other line has not filled within `gap_wait_ns` is requested from the re-request
  server. Requests are sized to what one reply holds (learned from the replies), pipelined up to `request_window`
  at a time, and retried on a timeout that adapts to the measured round trip (srtt + 4 rttvar, doubling per retry,
  clamped to `[min_request_timeout_ns, request_timeout_ns]`). The hole at the next needed sequence is always
  requested, whatever else is in flight.
- **Heartbeats and end of session** carry the next expected sequence, so a lost tail is noticed even when traffic
  stops. The session ends only after every message up to the advertised end has been delivered.
- **Late join.** ITCH has no snapshot, so with `join_from_start` (default) a late joiner replays from sequence 1
  through the re-request server while live packets keep arriving. The sink is told it is catching up; the
  consumer holds back the Aeron publishing until `CaughtUp`, then resends L2 snapshots to subscribers.
- **Sessions.** A new session after an ended one starts clean (the books are reset). A packet from a different
  session while one is active is counted and ignored (the spec says "abort"; one stray packet should not take down
  a healthy feed), and a silent session is replaced after `session_switch_timeout_ns`.
- **Stale.** If a hole makes no progress for `gap_wait + request_timeout * (attempts + 1)` (server silent, absent, or
  requests unsendable) or one request exhausts `max_request_attempts`, the session goes Stale: nothing after the
  hole is delivered, the consumer is told, and the books must not be trusted. `rebuild()` (or
  `auto_rebuild_after_ns`) replays from sequence 1.
- **Back-pressure.** If the ring is full nothing is dropped or reordered: the session holds the remainder, retries,
  and does not bother the server for data it already has.
- **Hostile input.** Malformed packets (truncated, trailing bytes, block lengths that do not add up) are rejected as
  a unit and counted; sequence 0 and absurdly large sequences are rejected as corrupt.

## Running it

```
# the transmitter stand-in: replay a real ITCH file as MoldUDP64 on two lines, with faults, and serve re-requests
gzip -dc data/12302019.NASDAQ_ITCH50.gz | build/itch_mold_sender \
    --a 127.0.0.1:30011 --b 127.0.0.1:30012 --rerequest-port 40500 --rate 300000 --max-messages 3000000 \
    --loss 0.05 --dup 0.02 --reorder 0.1:3 --burst 0.002:15 --delay-b 2 --corrupt 0.01

# the adapter (start it first, or later to see a mid-day join); --serve publishes the books over Aeron
build/itch_feed_handler --a 127.0.0.1:30011 --b 127.0.0.1:30012 --rerequest 127.0.0.1:40500 --digest [--serve]
```

For multicast use group addresses and `--iface <local ip>` on both. `itch_book_replay -n N --digest` prints the
digest a direct replay of the first N messages gives; the handler's `--digest` must equal it.

## The socket layer

`SocketManager` owns the network thread (pin it with `--cpu`; a pinned thread busy-polls). `UDPSocket` binds to
the multicast group address (so two lines can share a port without seeing each other's traffic), joins on the
chosen interface and requests a large receive buffer, warning if the OS capped it. `UdpReceiver` drains up to
`max_drain` datagrams per wakeup, backs off (1 ms doubling to 100 ms) on errors instead of spinning, declares the
socket failed after `max_consecutive_errors`, and `StopAndWait()` guarantees no callback runs after it returns.
`UnicastUDPSocket` is the re-request channel: replies come back to the socket that sent the request.

## Verification

| Test | What it shows |
|---|---|
| `test_moldudp64` | spec layout, whole-packet validation, builder budget, 400,000-input fuzz |
| `test_spsc_ring` | ordering, wrap-around, control reserve, 3,000,000 records across two threads |
| `test_feed_session` | 23 scripted scenarios, one behavior each, exact expectations |
| `test_feed_property` | 300 randomized runs by default (10,000 tried: 21M messages, 12.4M recovered): random streams, faults on both lines and the request path, late joins; the delivered stream must equal the original exactly |
| `test_udp_sockets` | real loopback UDP: batching, multicast isolation by group, stop semantics, failure reporting, 300 create/stop cycles |
| `test_feed_udp` | the whole path over real sockets (8 fault profiles incl. late joins and no server); books must equal a direct replay |

Eight deliberate bugs were put into `FeedSession` one at a time: seven were caught by these tests; the eighth is an
equivalent mutant (the removed condition could never be false at that point), which no test can detect. ASan/UBSan and ThreadSanitizer (`-DITCH_TSAN=ON`, separate build dir) are clean.

On the real 2019-12-30 data, 3,000,000 messages through two lines with 5% loss, loss bursts, duplicates,
reordering, a delayed line B and 1% corruption, and a re-request server that ignores 20% of requests, ended with
**byte-identical books** to a direct replay (digest `5ea98ba9a5e0a843`) after about 1,650 gaps and 4,000 requests.
A handler started when 900,000 messages had already been sent replayed them from the server in under a second
and also matched.

## Limits

- Everything was run on loopback on one macOS machine. Real multicast networks (IGMP, TTL, switch config),
  Linux, NIC timestamps and kernel bypass are untested, and the re-request path is only as realistic as the test
  server: a real exchange server rate-limits and may answer differently.
- The receive path uses the Boost.Asio reactor, not busy-polled kernel-bypass I/O. Latency has not been measured.
- Out-of-order and recovered packets are copied into the reorder buffer (heap), but only on gap paths; the in-order
  path hands data straight from the datagram to the ring. A zero-allocation claim has not been measured for the
  whole receive path.
- A hole unrecoverable after `max_request_attempts` (default 5) makes the session Stale. With a lossy request path
  raise `--attempts`; in the 20%-reply-loss test, 5 attempts failed one run of three and 12 passed all of them.
- SoupBinTCP and the trading-state handling of halts/auctions are not implemented.

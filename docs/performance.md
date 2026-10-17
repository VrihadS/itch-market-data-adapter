# Running it fast

What to build with, what to set at run time, and how to tune the host. Each section says whether the advice was
**measured here** or is **recommendation only**. Everything was developed and measured on one Apple M2 (8 cores,
8 GB, macOS 15.6.1) with no live Nasdaq connection. **None of the Linux host tuning below has been run or
measured by this project**; it is standard low-latency practice, listed so you can apply and verify it yourself.

## 1. Build

```sh
cmake --preset release-native            # Release + -march=native/-mcpu=native + LTO
cmake --build --preset release-native -j
ctest --preset release-native
```

| Preset | What it does |
|---|---|
| `release` | `-O3`-class Release build, portable binary. The default recommendation. |
| `release-native` | adds `ITCH_NATIVE` (tune for this CPU) and `ITCH_LTO` (whole-program optimisation). The binary only runs on CPUs like the build machine. |
| `profile` | `release-native` plus debug info and frame pointers, for `perf`/Instruments. |
| `no-aeron` | skips the Aeron fetch and build (no `--serve`, no md server/client). |
| `debug`, `asan`, `tsan` | correctness builds, not for timing. |

**Measured:** on the M2, `ITCH_NATIVE` + `ITCH_LTO` gave **no measurable speedup** over `release`. They are cheap
to try, but check on your own CPU instead of assuming a gain. Build on the machine that will run it, and never
time a `debug` or sanitizer build.

## 2. Runtime knobs (`itch_feed_handler`)

| Flag | Default | Guidance |
|---|---|---|
| `--cpu N` | off | Pins the network thread to core N, which then **busy-polls** (100 % of that core). Pick an isolated core on the NIC's NUMA node. macOS ignores affinity requests, so this only has effect on Linux. |
| `--ring-mb N` | 128 | Ring between the network thread and the book thread. It must absorb the book thread's worst stall. The ring is the only thing between a slow consumer and drops, so err large; it is address space until used. |
| `--rcvbuf-mb N` | 8 | Socket receive buffer. Raise it for bursty feeds; the kernel silently clamps it to `net.core.rmem_max` (see §4), so raise that first and read back what you got. |
| `--expect-orders N` | 4194304 | Peak simultaneously live orders. The 2019-12-30 sample peaks near 1.9 M. Sized correctly the order pool never touches the heap. The summary line `heap fallbacks` should read 0. Memory is about 44 bytes per expected order (≈ 185 MB at the default). |
| `--window N` | 8 | Re-requests in flight at once. Larger recovers big gaps faster but loads the re-request server. |
| `--gap-wait-ms N` | 5 | How long to wait for the other line before asking for a retransmit. Lower repairs sooner but sends more redundant requests when line B is merely a little late. |
| `--request-timeout-ms N`, `--attempts N` | 200, 5 | Initial retry timer (adapted from measured round-trip time) and how many tries before a hole is declared Stale. |
| `--stats-every S` | 2 | Counters print interval. Watch `bufdrop`, `gaps`, `retry`, `bad` in the status line, and `data-full`/`control-full` (ring overflows) and `heap fallbacks` in the final summary; all should be 0 on a healthy run. |

**Measured:** a single-thread book replay of the first 30 M messages (up to 09:50 ET, 8,906 books, 1.69 M live
orders) from a gzip pipe took 7.8 to 8.0 s over three runs, about 3.8 M messages/s **including** `gzip` and the
file reader, with the identical books digest every time (`d6b3f7c241fd7eda`) and 0 heap fallbacks. That is a
lower bound on the book path, not a latency figure. This is a replay from disk, not a live feed.

## 3. Verify tuning didn't change the answer

A faster configuration that changes results is a bug. After any change:

```sh
gzip -dc data/12302019.NASDAQ_ITCH50.gz | build/release-native/itch_book_replay -n 30000000 --digest
```

The digest must equal the one above (`d6b3f7c241fd7eda`) and `unapplied:` must be all zeros. For the live path,
run `itch_mold_sender` with `--loss`, `--dup`, `--reorder` against `itch_feed_handler --digest` and compare to the
no-fault run (see [feed.md](feed.md)). The test suite does this differentially (`ctest --preset release`).

## 4. Linux host tuning (recommendation only, unmeasured here)

Do these in order and measure after each; stop when the tail stops improving.

**CPU and scheduling**
- Isolate cores for the network and book threads: kernel args `isolcpus=<cores> nohz_full=<cores> rcu_nocbs=<cores>`,
  and keep housekeeping and IRQs elsewhere (`irqaffinity=<other cores>`).
- Set the governor to `performance`; disable deep C-states (e.g. `processor.max_cstate=1`, or BIOS) and turbo
  variance if you value a flat tail over peak clock.
- Pin with `--cpu` (network) and `taskset` or `numactl` (book thread and process). Keep both threads, the NIC and
  the memory on one NUMA node (`numactl --cpunodebind=N --membind=N`).
- Be careful with `chrt -f`: a real-time busy-poller on a core the kernel also needs can starve it. Use it only on
  isolated cores.

**Memory**
- Use transparent or explicit huge pages for the order pool and ring (`/sys/kernel/mm/transparent_hugepage/enabled`
  or `vm.nr_hugepages`) to cut TLB misses; verify with `perf stat -e dTLB-load-misses`.
- `ulimit -l unlimited` (memlock) if you lock memory; disable swap.

**Network (UDP multicast receive)**
- Raise `net.core.rmem_max` and `net.core.rmem_default` above the `--rcvbuf-mb` you ask for, and
  `net.core.netdev_max_backlog`. Check drops with `netstat -su` (`receive buffer errors`) and `ip -s link`.
- Reverse-path filtering can drop multicast: `net.ipv4.conf.<if>.rp_filter=2` (or 0) on the receiving interface.
  Raise `net.ipv4.igmp_max_memberships` if you join many groups.
- NIC: set RX ring size near the maximum (`ethtool -G`), turn off interrupt coalescing for latency
  (`ethtool -C rx-usecs 0`) or keep it for throughput, and steer the feed's flow to the pinned core's queue
  (RSS/flow steering, IRQ affinity; stop `irqbalance`). Check offloads with `ethtool -k`.
- Kernel bypass (Solarflare/Onload, DPDK, AF_XDP) is **not implemented**. The socket layer is plain POSIX UDP via
  Boost.Asio, so expect to replace `UDPSocket`/`UnicastUDPSocket` to use it.

## 5. Aeron media driver

The server can start an embedded driver for development. For anything you measure, run the standalone `aeronmd`
(built as `build/<preset>/_deps/aeron-build/binaries/aeronmd`) and give the server and clients
`--aeron-dir <dir>`. Driver settings are environment variables read by `aeronmd` (names confirmed in the Aeron 1.53.3
source; **values below are recommendations, not measured here**):

| Variable | Use |
|---|---|
| `AERON_DIR` | Driver directory. Put it on tmpfs (`/dev/shm/aeron`) on Linux, not a disk. |
| `AERON_THREADING_MODE` | `DEDICATED` (conductor, sender, receiver on their own threads) for lowest latency; `SHARED` uses one thread and fewer cores. |
| `AERON_CONDUCTOR_IDLE_STRATEGY`, `AERON_SENDER_IDLE_STRATEGY`, `AERON_RECEIVER_IDLE_STRATEGY`, `AERON_SHARED_IDLE_STRATEGY` | `noop`/busy-spin burns a core for the lowest latency; `yield`; `backoff` (the default) is the polite middle. Pin the driver threads to isolated cores too. |
| `AERON_TERM_BUFFER_LENGTH`, `AERON_IPC_TERM_BUFFER_LENGTH` | Larger terms tolerate slower subscribers before back-pressure. |
| `AERON_MTU_LENGTH`, `AERON_IPC_MTU_LENGTH` | Match the path MTU (jumbo frames only if the whole path supports them). |
| `AERON_SOCKET_SO_RCVBUF`, `AERON_SOCKET_SO_SNDBUF` | Socket buffers; subject to the same kernel caps as §4. |
| `AERON_DIR_DELETE_ON_START` | Clean start after a crash. |

The Aeron project's own [performance testing notes](https://github.com/aeron-io/aeron/wiki/Performance-Testing) and
[configuration reference](https://github.com/aeron-io/aeron/wiki/Configuration-Options) are the authority here.

## 6. Where more speed would come from (not implemented)

The book path allocates only in `std::map` price-level nodes (about 0.18 allocations per book message,
measured). Everything else on the hot path is pooled. Ranked by where I would look first, **each needs profiling
on your target hardware before you do it**:

1. Replace the `std::map` price levels with a flat or array-indexed ladder around the touch, which removes the
   remaining allocations and pointer chasing.
2. Replace the Boost.Asio receive path with a kernel-bypass stack (§4) once the NIC and kernel are tuned.
3. Batch the Aeron publish per packet instead of per book update when subscribers are slow.
4. Hand-prefetch the order and level for the next message in the batch.

## 7. How to measure

- Time the book path on **pre-decompressed** input so `gzip` isn't the bottleneck (the number in §2 is bounded by
  it): `gzip -dc day.gz > /fast/disk/day.itch`, then `itch_book_replay /fast/disk/day.itch`. Pin it, run several
  times and take the median.
- On Linux: `perf stat -e cycles,instructions,cache-misses,branch-misses,dTLB-load-misses <cmd>` for counters and
  `perf record -g` with the `profile` preset for hot spots. On macOS use Instruments' Time Profiler.
- Latency needs a cycle counter (`rdtsc` on x86 Linux). The macOS clock ticks about every 41 ns, so per-message
  timings there are quantised and only meaningful as averages over many messages.
- Report the median and the tail (p99, p99.9) of several runs, and say what machine and what input.

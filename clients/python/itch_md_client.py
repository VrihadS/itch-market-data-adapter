#!/usr/bin/env python3
"""
Pure-Python client for the ITCH market data gateway (standard library only).

Works on Windows, macOS and Linux with Python 3.8+. It makes ONE outbound TCP connection to the
gateway, so it needs no Aeron, no media driver and no inbound firewall rules.

    python itch_md_client.py --host 10.0.0.1 --port 40300 --symbols AAPL,MSFT --l2 5 --tns

As a library:

    from itch_md_client import MarketDataClient
    with MarketDataClient("10.0.0.1", 40300) as c:
        c.subscribe(["AAPL"], l2_depth=5, trades=True)
        for ev in c.events(seconds=30):
            if ev.type == "l2":    print(ev.symbol, ev.bids[0], ev.asks[0])
            elif ev.type == "trade": print(ev.symbol, ev.kind, ev.price, ev.shares)

Prices are integers with four implied decimals (2869100 == 286.9100); use price_str() to print them.
The wire format is documented in docs/messaging.md and matches include/itch/messaging/protocol.hpp.
"""
import argparse
import json
import random
import socket
import struct
import sys
import time

PROTOCOL_VERSION = 1
MAX_DEPTH = 10
MAX_SYMBOLS = 32
SYMBOL_LEN = 8
CHANNEL_LEN = 96

FEATURE_TRADES = 0x1      # time & sales
FEATURE_L2 = 0x2          # order book depth

MSG_SUBSCRIBE, MSG_UNSUBSCRIBE, MSG_ACK, MSG_HEARTBEAT, MSG_L2, MSG_TRADE = 1, 2, 3, 4, 10, 11

STATUS_TEXT = {
    0: "ok", 1: "unknown symbol", 2: "bad depth", 3: "bad features", 4: "bad reply channel",
    5: "too many clients", 6: "too many subscriptions", 7: "bad request", 8: "reply channel in use",
    9: "not subscribed", 10: "market data server unavailable",
}

# Little-endian, no padding: these mirror the packed C++ structs byte for byte.
_HEADER = struct.Struct("<HBB")
_SUBSCRIBE = struct.Struct("<HBBQIIBBBB%ds%ds" % (CHANNEL_LEN, MAX_SYMBOLS * SYMBOL_LEN))
_UNSUBSCRIBE = struct.Struct("<HBBQIBBH%ds" % (MAX_SYMBOLS * SYMBOL_LEN))
_ACK = struct.Struct("<HBBQIBBH" + "HBB" * MAX_SYMBOLS)
_HEARTBEAT = struct.Struct("<HBBQQB3x")
_L2 = struct.Struct("<HBBHBBB3xQQQ")
_LEVEL = struct.Struct("<iIQ")
_TRADE = struct.Struct("<HBBHccc3xQQQiIQQ")

assert _SUBSCRIBE.size == 376 and _UNSUBSCRIBE.size == 276 and _ACK.size == 148
assert _HEARTBEAT.size == 24 and _L2.size == 36 and _LEVEL.size == 16 and _TRADE.size == 60


def price_str(price):
    """Render an integer price with four implied decimals, e.g. 2869100 -> '286.9100'."""
    sign = "-" if price < 0 else ""
    price = abs(price)
    return "%s%d.%04d" % (sign, price // 10000, price % 10000)


def time_of_day(ns):
    """Format nanoseconds since midnight as HH:MM:SS.nnnnnnnnn."""
    s, n = divmod(ns, 1_000_000_000)
    return "%02d:%02d:%02d.%09d" % (s // 3600, s // 60 % 60, s % 60, n)


# ── encoding ──────────────────────────────────────────────────────────────

def _symbol_field(symbol):
    b = symbol.encode("ascii")
    if not 1 <= len(b) <= SYMBOL_LEN or any(c <= 0x20 or c > 0x7E for c in b):
        raise ValueError("bad symbol %r (1-8 printable characters, no spaces)" % symbol)
    return b.ljust(SYMBOL_LEN, b" ")


def _symbols_block(symbols):
    if not 0 <= len(symbols) <= MAX_SYMBOLS:
        raise ValueError("at most %d symbols per request" % MAX_SYMBOLS)
    block = b"".join(_symbol_field(s) for s in symbols)
    return block.ljust(MAX_SYMBOLS * SYMBOL_LEN, b"\0")


def encode_subscribe(client_id, request_id, symbols, features, depth):
    if not symbols:
        raise ValueError("no symbols")
    if features & FEATURE_L2 and not 1 <= depth <= MAX_DEPTH:
        raise ValueError("L2 depth must be 1..%d" % MAX_DEPTH)
    return _SUBSCRIBE.pack(_SUBSCRIBE.size, PROTOCOL_VERSION, MSG_SUBSCRIBE, client_id, request_id,
                           0, features, depth if features & FEATURE_L2 else 0, len(symbols), 0,
                           b"tcp",   # reply channel: required by the format, ignored by the gateway
                           _symbols_block(symbols))


def encode_unsubscribe(client_id, request_id, symbols, features):
    return _UNSUBSCRIBE.pack(_UNSUBSCRIBE.size, PROTOCOL_VERSION, MSG_UNSUBSCRIBE, client_id, request_id,
                             features, len(symbols), 0, _symbols_block(symbols))


def encode_heartbeat(client_id, send_ts_ns, from_server=0):
    return _HEARTBEAT.pack(_HEARTBEAT.size, PROTOCOL_VERSION, MSG_HEARTBEAT, client_id, send_ts_ns, from_server)


# ── events ────────────────────────────────────────────────────────────────

class Event:
    type = "event"

    def as_dict(self):
        d = {k: v for k, v in self.__dict__.items()}
        d["type"] = self.type
        return d


class Ack(Event):
    type = "ack"

    def __init__(self, request_id, status, results):
        self.request_id, self.status, self.results = request_id, status, results
        self.status_text = STATUS_TEXT.get(status, "status %d" % status)


class L2(Event):
    type = "l2"

    def __init__(self, locate, symbol, snapshot, sequence, exchange_ts_ns, send_ts_ns, bids, asks):
        self.locate, self.symbol, self.snapshot, self.sequence = locate, symbol, snapshot, sequence
        self.exchange_ts_ns, self.send_ts_ns = exchange_ts_ns, send_ts_ns
        self.bids, self.asks = bids, asks      # lists of (price, shares, orders), best first


class Trade(Event):
    type = "trade"

    def __init__(self, locate, symbol, kind, side, cross_type, sequence, exchange_ts_ns, send_ts_ns, price, shares, match_number):
        self.locate, self.symbol, self.kind, self.side, self.cross_type = locate, symbol, kind, side, cross_type
        self.sequence, self.exchange_ts_ns, self.send_ts_ns = sequence, exchange_ts_ns, send_ts_ns
        self.price, self.shares, self.match_number = price, shares, match_number


class Gap(Event):
    type = "gap"

    def __init__(self, locate, symbol, stream, expected, got):
        self.locate, self.symbol, self.stream, self.expected, self.got = locate, symbol, stream, expected, got


class Heartbeat(Event):
    type = "heartbeat"


class ServerTimeout(Event):
    type = "server_timeout"


# ── decoding ──────────────────────────────────────────────────────────────

def _decode_symbol(raw):
    raw = raw.split(b"\0", 1)[0].rstrip(b" ")
    return raw.decode("ascii", "replace")


def decode_ack(data):
    f = _ACK.unpack(data)       # f[3]=client_id f[4]=request_id f[5]=status f[6]=result_count f[7]=reserved, then (locate, status, reserved) x32
    count = f[6]
    if count > MAX_SYMBOLS:
        raise ValueError("ack with %d results" % count)
    results = [(f[8 + 3 * i], STATUS_TEXT.get(f[9 + 3 * i], "status %d" % f[9 + 3 * i])) for i in range(count)]
    return f[4], f[5], results       # request_id, status, [(locate, status text)]


def decode_l2(data):
    f = _L2.unpack_from(data, 0)
    locate, nb, na, snapshot, seq, exch, sent = f[3], f[4], f[5], f[6], f[7], f[8], f[9]
    if nb > MAX_DEPTH or na > MAX_DEPTH or len(data) != _L2.size + (nb + na) * _LEVEL.size:
        raise ValueError("malformed L2 message")
    levels = [_LEVEL.unpack_from(data, _L2.size + i * _LEVEL.size) for i in range(nb + na)]
    bids = [(p, s, o) for p, o, s in levels[:nb]]
    asks = [(p, s, o) for p, o, s in levels[nb:]]
    return locate, bool(snapshot), seq, exch, sent, bids, asks


def decode_trade(data):
    f = _TRADE.unpack(data)
    # f: length, version, type, locate, kind, side, cross, sequence, exch, sent, price, _, shares, match
    return (f[3], f[4].decode("ascii", "replace"), f[5].decode("ascii", "replace") if f[5] != b"\0" else "",
            f[6].decode("ascii", "replace") if f[6] != b"\0" else "", f[7], f[8], f[9], f[10], f[12], f[13])


# ── client ────────────────────────────────────────────────────────────────

class MarketDataClient:
    """One TCP session with the gateway. Not thread-safe: use it from one thread."""

    def __init__(self, host, port=40300, client_id=None, connect_timeout=5.0,
                 heartbeat_interval=1.0, server_timeout=5.0):
        self.host, self.port = host, port
        self.client_id = client_id or (random.getrandbits(63) | 1)
        self.connect_timeout = connect_timeout
        self.heartbeat_interval = heartbeat_interval
        self.server_timeout = server_timeout
        self._sock = None
        self._buf = bytearray()
        self._next_request = 1
        self._pending = {}                 # request_id -> symbols asked for
        self._names = {}                   # locate -> symbol
        self._expected = {}                # (locate, stream) -> next sequence
        self._last_heard = self._last_hb = time.monotonic()
        self._timeout_reported = False
        self.stats = {"l2": 0, "snapshots": 0, "trades": 0, "gaps": 0, "acks": 0, "heartbeats": 0}

    # context manager --------------------------------------------------------
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc):
        self.close()

    # connection ---------------------------------------------------------------
    def connect(self):
        self._sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock.settimeout(0.0)
        self._last_heard = self._last_hb = time.monotonic()

    def close(self):
        if self._sock is None:
            return
        try:
            self.disconnect()
        except OSError:
            pass
        try:
            self._sock.close()
        finally:
            self._sock = None

    def _send(self, data):
        sock = self._sock
        sock.settimeout(5.0)               # requests are tiny; a stalled send means a dead connection
        try:
            sock.sendall(data)
        finally:
            sock.settimeout(0.0)

    def _request_id(self):
        rid = self._next_request
        self._next_request += 1
        return rid

    # requests -----------------------------------------------------------------
    def subscribe(self, symbols, l2_depth=0, trades=False):
        """Ask for L2 depth (1..10) and/or time & sales on these symbols. Returns the request id.
        Subscribing again replaces the earlier request and sends a fresh L2 snapshot."""
        features = (FEATURE_L2 if l2_depth else 0) | (FEATURE_TRADES if trades else 0)
        if not features:
            raise ValueError("ask for l2_depth and/or trades")
        rid = self._request_id()
        self._send(encode_subscribe(self.client_id, rid, list(symbols), features, l2_depth))
        self._pending[rid] = list(symbols)
        return rid

    def unsubscribe(self, symbols, l2=False, trades=False):
        features = (FEATURE_L2 if l2 else 0) | (FEATURE_TRADES if trades else 0)
        if not features:
            raise ValueError("say what to drop: l2 and/or trades")
        rid = self._request_id()
        self._send(encode_unsubscribe(self.client_id, rid, list(symbols), features))
        self._pending[rid] = list(symbols)
        return rid

    def disconnect(self):
        """Tell the gateway to drop everything and close."""
        rid = self._request_id()
        self._send(encode_unsubscribe(self.client_id, rid, [], 0))
        return rid

    # receiving ----------------------------------------------------------------
    def poll(self, timeout=0.0):
        """Read what has arrived (waiting up to `timeout` seconds for the first byte).
        Returns a list of events."""
        events = []
        self._sock.settimeout(timeout if timeout > 0 else 0.0)
        try:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionError("gateway closed the connection")
            self._buf += chunk
            self._last_heard = time.monotonic()
            self._timeout_reported = False
        except (BlockingIOError, socket.timeout):
            pass
        finally:
            self._sock.settimeout(0.0)

        while len(self._buf) >= 4:
            length, version, mtype = _HEADER.unpack_from(self._buf, 0)
            if version != PROTOCOL_VERSION or length < 4 or length > 376:
                raise ValueError("protocol error from gateway (length %d, version %d)" % (length, version))
            if len(self._buf) < length:
                break
            data = bytes(self._buf[:length])
            del self._buf[:length]
            ev = self._dispatch(mtype, data)
            if ev is not None:
                events.extend(ev if isinstance(ev, list) else [ev])

        now = time.monotonic()
        if now - self._last_hb >= self.heartbeat_interval:
            self._last_hb = now
            self._send(encode_heartbeat(self.client_id, time.time_ns()))
        if not self._timeout_reported and now - self._last_heard > self.server_timeout:
            self._timeout_reported = True
            events.append(ServerTimeout())
        return events

    def events(self, seconds=None, poll_interval=0.05):
        """Generator of events until `seconds` elapse (forever if None)."""
        end = None if seconds is None else time.monotonic() + seconds
        while end is None or time.monotonic() < end:
            for ev in self.poll(timeout=poll_interval):
                yield ev

    def _name(self, locate):
        return self._names.get(locate, "")

    def _check_gap(self, locate, stream, seq, rebase):
        key = (locate, stream)
        out = []
        expected = self._expected.get(key)
        if not rebase and expected is not None and seq != expected:
            self.stats["gaps"] += 1
            out.append(Gap(locate, self._name(locate), stream, expected, seq))
        self._expected[key] = seq + 1
        return out

    def _dispatch(self, mtype, data):
        if mtype == MSG_ACK:
            request_id, status, results = decode_ack(data)
            symbols = self._pending.pop(request_id, [])
            for sym, (locate, st) in zip(symbols, results):
                if locate:
                    self._names[locate] = sym
                    if st == "ok":
                        self._expected.pop((locate, "trades"), None)
            self.stats["acks"] += 1
            return Ack(request_id, status, results)
        if mtype == MSG_L2:
            locate, snapshot, seq, exch, sent, bids, asks = decode_l2(data)
            self.stats["l2"] += 1
            self.stats["snapshots"] += snapshot
            out = self._check_gap(locate, "l2", seq, rebase=snapshot)
            out.append(L2(locate, self._name(locate), snapshot, seq, exch, sent, bids, asks))
            return out
        if mtype == MSG_TRADE:
            locate, kind, side, cross, seq, exch, sent, price, shares, match = decode_trade(data)
            self.stats["trades"] += 1
            out = self._check_gap(locate, "trades", seq, rebase=False)
            out.append(Trade(locate, self._name(locate), kind, side, cross, seq, exch, sent, price, shares, match))
            return out
        if mtype == MSG_HEARTBEAT:
            self.stats["heartbeats"] += 1
            return Heartbeat()
        raise ValueError("unexpected message type %d from gateway" % mtype)


# ── command line ──────────────────────────────────────────────────────────

def _print_event(ev, args):
    if args.json:
        print(json.dumps(ev.as_dict(), separators=(",", ":")), flush=True)
        return
    if ev.type == "ack":
        print("ack request %d: %s" % (ev.request_id, ev.status_text))
        for i, (locate, text) in enumerate(ev.results):
            print("   result %d: locate %d %s" % (i, locate, text))
    elif ev.type == "l2":
        print("\n%s  L2 %s seq %d  exch %s" % (ev.symbol or ev.locate, "SNAPSHOT" if ev.snapshot else "update",
                                              ev.sequence, time_of_day(ev.exchange_ts_ns)))
        for p, s, o in reversed(ev.asks):
            print("   ASK %12s x %-8d (%d)" % (price_str(p), s, o))
        print("   ---------")
        for p, s, o in ev.bids:
            print("   BID %12s x %-8d (%d)" % (price_str(p), s, o))
    elif ev.type == "trade":
        print("%s  TRADE %s %s %s x %d  match %d  seq %d  exch %s" % (
            ev.symbol or ev.locate, ev.kind, ev.side or "-", price_str(ev.price), ev.shares,
            ev.match_number, ev.sequence, time_of_day(ev.exchange_ts_ns)))
    elif ev.type == "gap":
        print("!! GAP %s %s: expected seq %d, got %d" % (ev.symbol or ev.locate, ev.stream, ev.expected, ev.got))
    elif ev.type == "server_timeout":
        print("!! no data from the gateway")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Client for the ITCH market data gateway")
    ap.add_argument("--host", default="127.0.0.1", help="gateway address")
    ap.add_argument("--port", type=int, default=40300, help="gateway TCP port")
    ap.add_argument("--symbols", required=True, help="comma-separated, e.g. AAPL,MSFT")
    ap.add_argument("--l2", type=int, default=0, metavar="DEPTH", help="order book depth 1..10")
    ap.add_argument("--tns", action="store_true", help="time & sales (prints)")
    ap.add_argument("--seconds", type=float, default=None, help="run time (default: until Ctrl-C)")
    ap.add_argument("--print-every", type=int, default=1, help="print every Nth L2 update (snapshots always print)")
    ap.add_argument("--json", action="store_true", help="one JSON object per line (prices are integers, 4 implied decimals)")
    ap.add_argument("--quiet", action="store_true", help="print only the summary")
    args = ap.parse_args(argv)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols or not (args.l2 or args.tns):
        ap.error("need --symbols and at least one of --l2 DEPTH / --tns")

    l2_seen = 0
    try:
        with MarketDataClient(args.host, args.port) as c:
            c.subscribe(symbols, l2_depth=args.l2, trades=args.tns)
            try:
                for ev in c.events(seconds=args.seconds):
                    if ev.type == "l2":
                        l2_seen += 1
                        if not ev.snapshot and l2_seen % max(1, args.print_every):
                            continue
                    if not args.quiet and ev.type != "heartbeat":
                        _print_event(ev, args)
            except KeyboardInterrupt:
                pass
            if not args.json:
                s = c.stats
                print("\nsummary: L2 messages %d (snapshots %d), trades %d, gaps %d, heartbeats %d" % (
                    s["l2"], s["snapshots"], s["trades"], s["gaps"], s["heartbeats"]))
    except BrokenPipeError:
        raise                                  # our own output pipe closed; handled at the bottom
    except (OSError, ConnectionError, ValueError) as e:
        print("error: %s" % e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BrokenPipeError:
        # Output was piped into something that exited early (e.g. `| head`): leave quietly.
        import os
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(0)

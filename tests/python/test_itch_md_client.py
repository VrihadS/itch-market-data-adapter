"""Python client tests: the shared wire vectors, framing, gap detection and the request builders.
Run:  python3 -m unittest discover -s tests/python -v   (no third-party packages needed)"""
import os
import re
import socket
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "clients", "python"))
import itch_md_client as md  # noqa: E402

VECTORS = os.path.join(os.path.dirname(__file__), "..", "golden", "wire_vectors.txt")


def load_vectors():
    out = {}
    with open(VECTORS) as f:
        for line in f:
            m = re.match(r'^(\w+) = "([0-9a-f]+)"$', line.strip())
            if m:
                out[m.group(1)] = bytes.fromhex(m.group(2))
    return out


class WireVectors(unittest.TestCase):
    """The same bytes the C++ encoder produces (test_messaging_protocol checks the C++ side)."""

    @classmethod
    def setUpClass(cls):
        cls.v = load_vectors()
        assert len(cls.v) == 6

    def test_client_to_server_messages_are_byte_identical(self):
        cid = 0x1122334455667788
        self.assertEqual(md.encode_subscribe(cid, 7, ["AAPL", "BRK.B"], md.FEATURE_L2 | md.FEATURE_TRADES, 5), self.v["SUBSCRIBE"])
        self.assertEqual(md.encode_unsubscribe(cid, 8, ["MSFT"], md.FEATURE_L2), self.v["UNSUBSCRIBE"])
        self.assertEqual(md.encode_heartbeat(cid, 1700000000123456789, from_server=1), self.v["HEARTBEAT"])   # the vector is the server-side form

    def test_ack(self):
        rid, status, results = md.decode_ack(self.v["ACK"])
        self.assertEqual((rid, status), (7, 0))
        self.assertEqual(results, [(13, "ok"), (0, "unknown symbol")])

    def test_l2(self):
        locate, snap, seq, exch, sent, bids, asks = md.decode_l2(self.v["L2"])
        self.assertEqual((locate, snap, seq, exch, sent), (13, True, 42, 36000123456789, 1700000000999999999))
        self.assertEqual(bids, [(2869300, 201, 2), (2869200, 300, 3), (2869100, 5, 1)])   # (price, shares, orders)
        self.assertEqual(asks, [(2869500, 100, 1), (2869600, 400, 4)])

    def test_trade(self):
        locate, kind, side, cross, seq, exch, sent, price, shares, match = md.decode_trade(self.v["TRADE"])
        self.assertEqual((locate, kind, side, cross, seq), (13, "Q", "", "C", 9))
        self.assertEqual((exch, sent, price, shares, match), (57600000000000, 1700000001000000000, 2870000, 5000000000, 123456))

    def test_sizes_match_the_c_structs(self):
        self.assertEqual([len(self.v[k]) for k in ("SUBSCRIBE", "UNSUBSCRIBE", "HEARTBEAT", "ACK", "L2", "TRADE")],
                         [376, 276, 24, 148, 36 + 5 * 16, 60])


class Helpers(unittest.TestCase):
    def test_price_and_time_formatting(self):
        self.assertEqual(md.price_str(2869100), "286.9100")
        self.assertEqual(md.price_str(5), "0.0005")
        self.assertEqual(md.price_str(-12345), "-1.2345")
        self.assertEqual(md.time_of_day(36000123456789), "10:00:00.123456789")

    def test_request_validation(self):
        with self.assertRaises(ValueError): md.encode_subscribe(1, 1, [], md.FEATURE_TRADES, 0)
        with self.assertRaises(ValueError): md.encode_subscribe(1, 1, ["AAPL"], md.FEATURE_L2, 11)
        with self.assertRaises(ValueError): md.encode_subscribe(1, 1, ["AAPL"], md.FEATURE_L2, 0)
        with self.assertRaises(ValueError): md.encode_subscribe(1, 1, ["TOOLONGSYM"], md.FEATURE_TRADES, 0)
        with self.assertRaises(ValueError): md.encode_subscribe(1, 1, ["A B"], md.FEATURE_TRADES, 0)
        with self.assertRaises(ValueError): md.encode_subscribe(1, 1, ["A"] * 33, md.FEATURE_TRADES, 0)

    def test_malformed_input_is_rejected(self):
        with self.assertRaises(Exception): md.decode_l2(md.encode_heartbeat(1, 1))
        v = load_vectors()
        bad = bytearray(v["L2"]); bad[md._L2.size - 28 + 4 - 4] = 11     # inflate bid_count
        bad[4 + 2] = 11
        with self.assertRaises(ValueError): md.decode_l2(bytes(bad))


class FakeGateway:
    """A TCP server that sends canned byte chunks (with pauses, to exercise stream reassembly),
    records what the client sent, and optionally closes the connection when done."""

    def __init__(self, chunks, close_when_done=False, linger=1.0):
        self.srv = socket.socket(); self.srv.bind(("127.0.0.1", 0)); self.srv.listen(1)
        self.port = self.srv.getsockname()[1]
        self.chunks, self.close_when_done, self.linger = ([chunks] if isinstance(chunks, bytes) else chunks), close_when_done, linger
        self.received = b""
        self.t = threading.Thread(target=self.run, daemon=True); self.t.start()

    def run(self):
        c, _ = self.srv.accept()
        try:
            for chunk in self.chunks:
                c.sendall(chunk)
                time.sleep(0.05)
            if self.close_when_done:
                return
            c.settimeout(self.linger)
            while True:
                d = c.recv(4096)
                if not d: break
                self.received += d
        except OSError:
            pass
        finally:
            c.close(); self.srv.close()


def l2_msg(locate, seq, snapshot=False):
    b = bytearray(load_vectors()["L2"])
    b[4:6] = locate.to_bytes(2, "little"); b[8] = 1 if snapshot else 0       # locate @4, snapshot flag @8
    b[12:20] = seq.to_bytes(8, "little")                                     # sequence @12
    return bytes(b)


def ack_for(request_id):
    b = bytearray(load_vectors()["ACK"])
    b[12:16] = request_id.to_bytes(4, "little")                              # request id @12
    return bytes(b)


class ClientBehaviour(unittest.TestCase):
    def run_client(self, chunks, seconds=0.7):
        gw = FakeGateway(chunks)
        with md.MarketDataClient("127.0.0.1", gw.port, client_id=99, heartbeat_interval=0.1) as c:
            c.subscribe(["AAPL", "NOPE"], l2_depth=3, trades=True)
            evs = list(c.events(seconds=seconds, poll_interval=0.02))
            stats = dict(c.stats)
        gw.t.join(3.0)
        return evs, stats, gw

    def test_reassembles_a_split_stream_names_symbols_and_talks_back(self):
        v = load_vectors()
        stream = ack_for(1) + l2_msg(13, 1, True) + v["TRADE"] + v["HEARTBEAT"]
        cut = len(ack_for(1)) + 10                                  # split in the middle of the L2 message
        evs, stats, gw = self.run_client([stream[:5], stream[5:cut], stream[cut:]])
        self.assertEqual([e.type for e in evs if e.type != "heartbeat"], ["ack", "l2", "trade"])
        l2 = [e for e in evs if e.type == "l2"][0]
        self.assertEqual((l2.locate, l2.snapshot, l2.sequence, l2.symbol), (13, True, 1, "AAPL"))   # named via the ack
        self.assertEqual([e.symbol for e in evs if e.type == "trade"], ["AAPL"])
        self.assertGreaterEqual(stats["heartbeats"], 1)
        # what the client sent: the subscribe request first, heartbeats, and a disconnect on exit
        sub = md.encode_subscribe(99, 1, ["AAPL", "NOPE"], md.FEATURE_L2 | md.FEATURE_TRADES, 3)
        self.assertTrue(gw.received.startswith(sub))
        self.assertIn(md.encode_unsubscribe(99, 2, [], 0), gw.received)
        self.assertIn(md.encode_heartbeat(99, 0)[:12], gw.received)         # header + client id of a heartbeat

    def test_gap_detection_and_snapshot_rebase(self):
        stream = ack_for(1) + l2_msg(13, 1, True) + l2_msg(13, 2) + l2_msg(13, 5) + l2_msg(13, 6) + l2_msg(13, 50, True) + l2_msg(13, 51)
        evs, stats, _ = self.run_client(stream)
        gaps = [e for e in evs if e.type == "gap"]
        self.assertEqual(len(gaps), 1)
        self.assertEqual((gaps[0].expected, gaps[0].got, gaps[0].stream), (3, 5, "l2"))
        self.assertEqual(stats["gaps"], 1)        # the snapshot at seq 50 re-bases instead of reporting a gap

    def test_trade_sequence_gaps_are_reported_too(self):
        def trade(seq):
            b = bytearray(load_vectors()["TRADE"]); b[12:20] = seq.to_bytes(8, "little"); return bytes(b)   # sequence @12
        evs, stats, _ = self.run_client(ack_for(1) + trade(1) + trade(2) + trade(4))
        gaps = [e for e in evs if e.type == "gap"]
        self.assertEqual([(g.stream, g.expected, g.got) for g in gaps], [("trades", 3, 4)])

    def test_protocol_errors_raise(self):
        gw = FakeGateway(bytes([0x10, 0x00, 99, 1]) + b"\0" * 12)       # wrong protocol version
        with md.MarketDataClient("127.0.0.1", gw.port) as c:
            with self.assertRaises(ValueError):
                list(c.events(seconds=1.0, poll_interval=0.02))

    def test_closed_connection_raises(self):
        gw = FakeGateway(b"", close_when_done=True)
        with md.MarketDataClient("127.0.0.1", gw.port) as c:
            with self.assertRaises((ConnectionError, OSError)):
                list(c.events(seconds=1.0, poll_interval=0.02))

    def test_refused_connection_surfaces_as_oserror(self):
        with self.assertRaises(OSError):
            md.MarketDataClient("127.0.0.1", 1, connect_timeout=0.5).connect()

    def test_silent_server_is_reported_once(self):
        gw = FakeGateway(b"")
        with md.MarketDataClient("127.0.0.1", gw.port, server_timeout=0.2, heartbeat_interval=10) as c:
            evs = list(c.events(seconds=0.7, poll_interval=0.02))
        self.assertEqual([e.type for e in evs].count("server_timeout"), 1)


if __name__ == "__main__":
    unittest.main()

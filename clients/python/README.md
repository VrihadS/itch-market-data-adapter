# Python client (Windows / macOS / Linux)

`itch_md_client.py` is a single file, **standard library only**, Python 3.8 or newer. It makes one
outbound TCP connection to the gateway (`itch_md_gateway`) that runs next to the market data server.
No Aeron, no media driver, no inbound firewall rules on the client machine.

## Windows quick start

1. Install Python 3 from python.org (tick "Add python.exe to PATH"), or use the `py` launcher.
2. Copy `itch_md_client.py` to the machine. Nothing else is needed.
3. Check you can reach the gateway (PowerShell): `Test-NetConnection <gateway ip> -Port 40300`
4. Run it:

       py itch_md_client.py --host <gateway ip> --port 40300 --symbols AAPL,MSFT --l2 5 --tns

   `--l2 N` asks for order book depth 1..10, `--tns` for time & sales; use either or both. Ctrl-C stops it.

The same command works on macOS and Linux (`python3 itch_md_client.py ...`).

The gateway machine needs the TCP port open (default 40300) and should be started with `--listen 0.0.0.0`
plus `--allow <your network>`; see [docs/messaging.md](../../docs/messaging.md).

## As a library

```python
from itch_md_client import MarketDataClient, price_str

with MarketDataClient("10.0.0.1", 40300) as c:
    c.subscribe(["AAPL", "MSFT"], l2_depth=10, trades=True)
    for ev in c.events(seconds=60):
        if ev.type == "l2":
            best_bid, best_ask = ev.bids[0], ev.asks[0]            # (price, shares, orders)
            print(ev.symbol, price_str(best_bid[0]), price_str(best_ask[0]), "snapshot" if ev.snapshot else "")
        elif ev.type == "trade":
            print(ev.symbol, ev.kind, ev.side, price_str(ev.price), ev.shares)
        elif ev.type == "gap":
            print("missed updates:", ev.symbol, ev.expected, "->", ev.got)   # re-subscribe to get a fresh snapshot
```

Events: `ack`, `l2`, `trade`, `gap`, `heartbeat`, `server_timeout`. Prices are integers with four implied
decimals (`2869100` is 286.9100). L2 levels are `(price, shares, orders)`, best first. `l2` events carry the
whole top-N book each time, so keeping the latest one is enough. Trade `kind`: `E` execution, `C` execution
at a different price, `P` non-displayed trade, `Q` cross (see `cross_type`), `B` broken trade.

`--json` prints one JSON object per line, ready for `pandas.read_json(..., lines=True)`.

## Things to know

- The client does not reconnect by itself. If the gateway restarts or closes the connection
  (`ConnectionError`), create a new `MarketDataClient` and subscribe again; you get a fresh snapshot.
- A `gap` means updates were missed. For L2, the next update is still complete, and a re-subscribe
  returns a snapshot. Missed trades are reported but not replayed.
- TCP adds a hop and can delay data behind a slow reader. This path suits dashboards, research and
  strategies that tolerate milliseconds. For microseconds use the C++ client over Aeron.
- It has been tested on macOS against the real gateway and a Python 3.14 interpreter. It uses only portable
  standard-library calls, but it has **not** been run on Windows yet.

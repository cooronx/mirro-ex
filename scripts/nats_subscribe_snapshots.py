#!/usr/bin/env python3
"""订阅 NATS 盘口并演示最简单的自动买入、卖出和撤单。"""

import argparse
import asyncio
import json
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from marketdata_pb2 import Envelope

NATS_URL = "nats://127.0.0.1:4222"
NATS_SUBJECT = "market.snapshot"
API_URL = "http://127.0.0.1:5800"
USERNAME = "python_strategy"
PASSWORD = "python_strategy"
INITIAL_CASH = 1_000_000 * 10_000
ORDER_QTY = 100
DECISION_INTERVAL_MS = 3_000
STALE_ORDER_MS = 60_000
ACTIVE_STATUSES = {"new", "working", "partially_filled"}

class ApiError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(f"API error {code}: {message}")
        self.code = code

async def api(path, payload=None, query=None):
    def request():
        url = API_URL + path
        if query:
            url += "?" + urlencode(query)
        body = None if payload is None else json.dumps(payload).encode()
        headers = {"Content-Type": "application/json"} if body else {}
        req = Request(url, data=body, headers=headers, method="POST" if body else "GET")
        try:
            with urlopen(req, timeout=5) as response:
                result = json.load(response)
        except URLError as exc:
            raise RuntimeError(f"cannot reach {API_URL}: {exc}") from exc
        if result.get("code") != 1 or result.get("data") is None:
            raise ApiError(result.get("code"), result.get("msg"))
        return result["data"]
    return await asyncio.to_thread(request)

async def login_or_create_account():
    credentials = {"username": USERNAME, "password": PASSWORD}
    try:
        return await api("/trading/login", credentials)
    except ApiError as exc:
        if exc.code != 2407:
            raise
    try:
        return await api("/trading/accounts", {**credentials, "initial_cash": INITIAL_CASH})
    except ApiError as exc:
        if exc.code != 2409:
            raise
        return await api("/trading/login", credentials)

class SimpleStrategy:
    def __init__(self, user_id, code, dry_run):
        self.user_id = user_id
        self.code = code
        self.dry_run = dry_run
        self.last_decision_ms = 0

    async def on_snapshot(self, snapshot):
        if snapshot.code != self.code or not snapshot.bids or not snapshot.asks:
            return
        now = snapshot.event_ts_ms
        if now < self.last_decision_ms:
            self.last_decision_ms = 0
        if now - self.last_decision_ms < DECISION_INTERVAL_MS:
            return
        self.last_decision_ms = now
        try:
            orders, positions = await asyncio.gather(
                api("/trading/orders", query={"user_id": self.user_id}),
                api("/trading/positions", query={"user_id": self.user_id, "code": self.code}),
            )
            active = next(
                (o for o in orders if o["code"] == self.code and o["status"] in ACTIVE_STATUSES),
                None,
            )
            if active:
                if now - active["created_at"] >= STALE_ORDER_MS:
                    await self.cancel(active["order_id"])
                return

            position = positions[0] if positions else None
            available = position["available_qty"] if position else 0
            if available > 0:
                await self.order("sell", snapshot.bids[0].price, min(ORDER_QTY, available))
            else:
                await self.order("buy", snapshot.asks[0].price, ORDER_QTY)
        except (ApiError, RuntimeError) as exc:
            print(f"decision skipped: {exc}")

    async def order(self, side, price, qty):
        print(f"{side} {self.code} price={price / 10000:.4f} qty={qty}")
        if not self.dry_run:
            await api(
                "/trading/orders",
                {"user_id": self.user_id, "code": self.code, "side": side, "price": price, "qty": qty},
            )

    async def cancel(self, order_id):
        print(f"cancel {order_id}")
        if not self.dry_run:
            await api(
                "/trading/orders/cancel",
                {"user_id": self.user_id, "order_id": order_id},
            )

async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("code", help="证券代码，例如 300274.XSHE")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--print-snapshots", action="store_true")
    args = parser.parse_args()
    try:
        from nats.aio.client import Client as NATS
    except ModuleNotFoundError as exc:
        raise SystemExit("run: python -m pip install nats-py protobuf") from exc
    account = await login_or_create_account()
    strategy = SimpleStrategy(account["user_id"], args.code.strip(), args.dry_run)
    nc = NATS()
    await nc.connect(NATS_URL)
    async def handle(message):
        envelope = Envelope()
        envelope.ParseFromString(message.data)
        if envelope.WhichOneof("payload") != "snapshot":
            return
        snapshot = envelope.snapshot
        if args.print_snapshots and snapshot.code == strategy.code:
            print(f"snapshot {snapshot.event_ts_ms} {snapshot.code}")
        await strategy.on_snapshot(snapshot)
    await nc.subscribe(NATS_SUBJECT, cb=handle)
    print(f"subscribed {NATS_SUBJECT}, account={account['user_id']}, code={strategy.code}")
    await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

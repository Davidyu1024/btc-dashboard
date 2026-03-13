#!/usr/bin/env python3
"""Hyperliquid liquidation collector via Hypedexer feed.

- Connects to wss://api.hypedexer.com/ws/v1/liquidation with Authorization header
- Subscribes to all_liquidations stream
- Aggregates events into 5-minute Beijing-time windows
- Writes daily CSV files under data/hyperliquid_liquidations/YYYY-MM-DD.csv
- Keeps only the most recent 3 days of files

Environment variables:
  HYPEDEXER_API_KEY  (required) -- Hypedexer API key (starts with hl_live_...)
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

import websockets
from zoneinfo import ZoneInfo

HYPEDEXER_WS_URL = "wss://api.hypedexer.com/ws/v1/liquidation"
SUBSCRIPTION_MSG = {
    "op": "subscribe",
    "channel": "all_liquidations",
}
WINDOW_MINUTES = 5
RETENTION_DAYS = 3
BJT = ZoneInfo("Asia/Shanghai")
DATA_DIR = Path("data/hyperliquid_liquidations")
DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)


@dataclass
class LqStat:
    count: int = 0
    total_sz: float = 0.0
    total_notional: float = 0.0
    price_min: float | None = None
    price_max: float | None = None

    def update(self, px: float, sz: float) -> None:
        self.count += 1
        self.total_sz += sz
        self.total_notional += px * sz
        if self.price_min is None or px < self.price_min:
            self.price_min = px
        if self.price_max is None or px > self.price_max:
            self.price_max = px


class LiquidationAggregator:
    def __init__(self, window_minutes: int = WINDOW_MINUTES) -> None:
        self.window_minutes = window_minutes
        self.current_window_start: datetime | None = None
        self.stats: Dict[Tuple[str, str], LqStat] = defaultdict(LqStat)

    def _align_window(self, ts: datetime) -> datetime:
        aligned_minute = (ts.minute // self.window_minutes) * self.window_minutes
        return ts.replace(minute=aligned_minute, second=0, microsecond=0)

    def add_event(self, ts: datetime, coin: str, side: str, px: float, sz: float) -> None:
        ts_bjt = ts.astimezone(BJT)
        window_start = self._align_window(ts_bjt)
        if self.current_window_start is None:
            self.current_window_start = window_start
        elif window_start > self.current_window_start:
            # flush finished windows before processing new one
            self._flush_until(window_start)
        key = (coin.upper(), side.upper())
        self.stats[key].update(px, sz)

    def _flush_until(self, new_window: datetime) -> None:
        while self.current_window_start is not None and self.current_window_start < new_window:
            self.flush_current_window()
            self.current_window_start += timedelta(minutes=self.window_minutes)

    def flush_current_window(self) -> None:
        if self.current_window_start is None or not self.stats:
            # nothing to flush
            return
        window_start = self.current_window_start
        date_str = window_start.date().isoformat()
        file_path = DATA_DIR / f"{date_str}.csv"
        file_exists = file_path.exists()
        with file_path.open("a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(
                    [
                        "window_start_bjt",
                        "symbol",
                        "side",
                        "liq_count",
                        "total_sz",
                        "total_notional_usd",
                        "price_min",
                        "price_max",
                    ]
                )
            for (symbol, side), stat in sorted(self.stats.items()):
                writer.writerow(
                    [
                        window_start.isoformat(),
                        symbol,
                        side,
                        stat.count,
                        f"{stat.total_sz:.6f}",
                        f"{stat.total_notional:.2f}",
                        f"{stat.price_min:.2f}" if stat.price_min is not None else "",
                        f"{stat.price_max:.2f}" if stat.price_max is not None else "",
                    ]
                )
        logging.info("flushed window %s with %d symbol/side entries", window_start, len(self.stats))
        self.stats.clear()
        cleanup_old_files()


def cleanup_old_files(retention_days: int = RETENTION_DAYS) -> None:
    cutoff = datetime.now(tz=BJT).date() - timedelta(days=retention_days)
    for file in DATA_DIR.glob("*.csv"):
        try:
            file_date = datetime.strptime(file.stem, "%Y-%m-%d").date()
        except ValueError:
            continue
        if file_date < cutoff:
            logging.info("removing old file %s", file)
            file.unlink(missing_ok=True)


async def consume_liquidations(api_key: str) -> None:
    aggregator = LiquidationAggregator()
    while True:
        try:
            async with websockets.connect(
                HYPEDEXER_WS_URL,
                extra_headers={"Authorization": f"Bearer {api_key}"},
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                logging.info("connected to Hypedexer feed")
                await ws.send(json.dumps(SUBSCRIPTION_MSG))
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        logging.warning("invalid JSON: %s", raw)
                        continue
                    if msg.get("type") == "subscribed":
                        logging.info("subscription ack: %s", msg)
                        continue
                    if msg.get("type") != "liq":
                        continue
                    ts_ms = msg.get("ts") or msg.get("time")
                    event_ts = datetime.fromtimestamp(ts_ms / 1000, tz=BJT) if ts_ms else datetime.now(tz=BJT)
                    coin = msg.get("coin", "UNKNOWN")
                    side = msg.get("side", "UNKNOWN")
                    px = float(msg.get("px", 0))
                    sz = float(msg.get("sz", 0))
                    aggregator.add_event(event_ts, coin, side, px, sz)
        except Exception as exc:
            logging.exception("feed error, reconnecting: %s", exc)
            await asyncio.sleep(5)


def main() -> None:
    api_key = os.getenv("HYPEDEXER_API_KEY")
    if not api_key:
        raise SystemExit("HYPEDEXER_API_KEY environment variable is required")
    logging.info("starting Hyperliquid liquidation collector")
    try:
        asyncio.run(consume_liquidations(api_key))
    except KeyboardInterrupt:
        logging.info("shutting down")


if __name__ == "__main__":
    main()

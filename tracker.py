#!/usr/bin/env python3
"""
Polymarket Activity Tracker (address-based) with trade status updates.

What it does:
- Track Polymarket activity for a given 0x... address via data-api.
- Store raw events to JSONL + optionally to SQLite.
- Maintain running aggregates and write a JSON summary snapshot.
- Print a status line every time a NEW TRADE is logged.

Outputs (relative to --out-dir):
- <out-dir>/raw_events.jsonl
- <out-dir>/snapshot_summary.json
And if you pass --sqlite:
- <sqlite path> (e.g. data/events.sqlite)

Run example:
python tracker.py --address 0x... --minutes 30 --poll 7 --lookback 120 --warmup 600 --out-dir data --sqlite data/events.sqlite
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

DATA_BASE_URL = "https://data-api.polymarket.com"

POLL_SECONDS_DEFAULT = 7
LOOKBACK_SECONDS_DEFAULT = 120  # used as overlap (safety buffer)
PAGE_LIMIT_DEFAULT = 200
WARMUP_SECONDS_DEFAULT = 600

RAW_EVENTS_FILENAME = "raw_events.jsonl"
SUMMARY_FILENAME = "snapshot_summary.json"

HTTP_TIMEOUT_SECONDS = 15
MAX_RETRIES = 5


def utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_address(addr: str) -> str:
    addr = addr.strip()
    if not addr.lower().startswith("0x"):
        raise ValueError("Address must start with 0x...")
    if not re.fullmatch(r"0x[a-fA-F0-9]{40}", addr):
        raise ValueError("Address must be a valid 20-byte hex address (0x + 40 hex chars).")
    return addr


def _as_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except (ValueError, TypeError):
        return None


def _as_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def compute_dedupe_key(event: Dict[str, Any]) -> str:
    """
    Dedupe key priority:
    1) id
    2) tx_hash:log_index
    3) tx_hash:payload_hash
    4) payload_hash
    """
    if event.get("id"):
        return str(event["id"])

    tx_hash = event.get("transactionHash") or event.get("transaction_hash") or event.get("txHash")
    log_index = event.get("logIndex") or event.get("log_index")

    if tx_hash and log_index is not None:
        return f"{tx_hash}:{log_index}"

    stable_keys = [
        "timestamp",
        "type",
        "conditionId",
        "outcome",
        "side",
        "price",
        "size",
        "usdcSize",
        "maker",
        "taker",
        "trader",
        "user",
        "owner",
        "orderHash",
        "tradeId",
        "tokenId",
        "market",
        "asset",
        "nonce",
    ]
    stable_payload = {k: event.get(k) for k in stable_keys if k in event}
    payload = json.dumps(stable_payload, sort_keys=True, separators=(",", ":"))
    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    if tx_hash:
        return f"{tx_hash}:{payload_hash}"
    return payload_hash


def log_trade(event: Dict[str, Any]) -> None:
    """Print a one-line status update for each newly logged TRADE event."""
    ts = _as_int(event.get("timestamp"))
    iso = utc_iso(ts) if ts is not None else "unknown-time"

    side = str(event.get("side", "")).upper() or "?"
    outcome = str(event.get("outcome", "?"))
    size = _as_float(event.get("size"))
    price = _as_float(event.get("price"))
    usdc = _as_float(event.get("usdcSize"))
    condition_id = event.get("conditionId") or "?"
    slug = event.get("slug")
    title = event.get("title")

    # Keep it compact but informative
    market_label = slug or (title[:60] + "…" if isinstance(title, str) and len(title) > 60 else title) or condition_id
    size_str = f"{size:.6f}".rstrip("0").rstrip(".") if size is not None else "?"
    price_str = f"{price:.6f}".rstrip("0").rstrip(".") if price is not None else "?"
    usdc_str = f"{usdc:.6f}".rstrip("0").rstrip(".") if usdc is not None else "?"

    print(
        f"[TRADE] {iso} {side:<4} outcome={outcome} size={size_str} price={price_str} usdc={usdc_str} | market={market_label}"
    )


class DataClient:
    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def fetch_activity(
        self, wallet: str, start_ts: int, end_ts: int, limit: int = PAGE_LIMIT_DEFAULT
    ) -> List[Dict[str, Any]]:
        url = f"{DATA_BASE_URL}/activity"
        offset = 0
        events: List[Dict[str, Any]] = []

        while True:
            params = {
                "user": wallet,
                "start": start_ts,
                "end": end_ts,
                "limit": limit,
                "offset": offset,
                "sortBy": "TIMESTAMP",
                "sortDirection": "ASC",
            }
            resp = self.session.get(url, params=params, timeout=HTTP_TIMEOUT_SECONDS)
            resp.raise_for_status()

            batch = resp.json()
            if isinstance(batch, dict):
                batch = batch.get("data", [])
            if not isinstance(batch, list):
                batch = []

            events.extend(batch)
            if len(batch) < limit:
                break
            offset += limit

        return events


class EventStore:
    def __init__(self, out_dir: str, sqlite_path: Optional[str] = None, commit_every: int = 200) -> None:
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)

        self.raw_path = os.path.join(out_dir, RAW_EVENTS_FILENAME)
        self.sqlite_path = sqlite_path
        self.commit_every = max(1, commit_every)

        self._seen: set[str] = set()
        self._db: Optional[sqlite3.Connection] = None
        self._pending_inserts = 0

        if sqlite_path:
            self._init_db(sqlite_path)

    def _init_db(self, path: str) -> None:
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL;")
        self._db.execute("PRAGMA synchronous=NORMAL;")
        self._db.execute("PRAGMA temp_store=MEMORY;")
        self._db.execute("PRAGMA cache_size=-20000;")  # ~20MB

        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                dedupe_key TEXT PRIMARY KEY,
                ts INTEGER,
                type TEXT,
                slug TEXT,
                conditionId TEXT,
                outcome TEXT,
                side TEXT,
                price REAL,
                size REAL,
                usdcSize REAL,
                txHash TEXT,
                logIndex INTEGER,
                raw_json TEXT
            )
            """
        )
        self._db.commit()

        # Load existing keys to dedupe across restarts.
        cur = self._db.cursor()
        cur.execute("SELECT dedupe_key FROM events")
        self._seen.update(row[0] for row in cur.fetchall())

    def close(self) -> None:
        if self._db:
            try:
                self._db.commit()
            finally:
                self._db.close()
                self._db = None

    def seen(self, dedupe_key: str) -> bool:
        return dedupe_key in self._seen

    def add(self, event: Dict[str, Any], dedupe_key: str) -> None:
        self._seen.add(dedupe_key)

        # Append raw event JSONL (always)
        payload = dict(event)
        payload["__dedupe_key"] = dedupe_key
        with open(self.raw_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, separators=(",", ":")) + "\n")

        if not self._db:
            return

        ts = _as_int(event.get("timestamp"))
        typ = event.get("type")
        slug = event.get("slug")
        condition_id = event.get("conditionId")
        outcome = event.get("outcome")
        side = event.get("side")
        price = _as_float(event.get("price"))
        size = _as_float(event.get("size"))
        usdc_size = _as_float(event.get("usdcSize"))
        tx_hash = event.get("transactionHash") or event.get("transaction_hash") or event.get("txHash")
        log_index = _as_int(event.get("logIndex") or event.get("log_index"))

        self._db.execute(
            """
            INSERT OR IGNORE INTO events (
                dedupe_key, ts, type, slug, conditionId, outcome, side, price, size, usdcSize, txHash, logIndex, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dedupe_key,
                ts,
                typ,
                slug,
                condition_id,
                outcome,
                side,
                price,
                size,
                usdc_size,
                tx_hash,
                log_index,
                json.dumps(event, separators=(",", ":")),
            ),
        )

        self._pending_inserts += 1
        if self._pending_inserts >= self.commit_every:
            self._db.commit()
            self._pending_inserts = 0


@dataclass
class TradeAggregate:
    condition_id: str
    outcome: str
    slug: Optional[str]
    title: Optional[str]
    n_trades: int = 0
    buy_usdc: float = 0.0
    sell_usdc: float = 0.0
    buy_shares: float = 0.0
    sell_shares: float = 0.0
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None

    def update(self, event: Dict[str, Any]) -> None:
        self.n_trades += 1
        side = str(event.get("side", "")).lower()
        price = _as_float(event.get("price"))
        size = _as_float(event.get("size")) or 0.0
        usdc_size = _as_float(event.get("usdcSize")) or 0.0
        timestamp = _as_int(event.get("timestamp"))

        if side == "buy":
            self.buy_shares += size
            self.buy_usdc += usdc_size
        elif side == "sell":
            self.sell_shares += size
            self.sell_usdc += usdc_size

        if price is not None:
            if self.min_price is None or price < self.min_price:
                self.min_price = price
            if self.max_price is None or price > self.max_price:
                self.max_price = price

        if timestamp is not None:
            if self.first_ts is None or timestamp < self.first_ts:
                self.first_ts = timestamp
            if self.last_ts is None or timestamp > self.last_ts:
                self.last_ts = timestamp

        # Fill metadata if missing
        if not self.slug and event.get("slug"):
            self.slug = event.get("slug")
        if not self.title and event.get("title"):
            self.title = event.get("title")

    def to_snapshot(self) -> Dict[str, Any]:
        net_shares = self.buy_shares - self.sell_shares
        buy_vwap = (self.buy_usdc / self.buy_shares) if self.buy_shares else None
        sell_vwap = (self.sell_usdc / self.sell_shares) if self.sell_shares else None
        return {
            "conditionId": self.condition_id,
            "slug": self.slug,
            "title": self.title,
            "outcome": self.outcome,
            "n_trades": self.n_trades,
            "buy_shares": round(self.buy_shares, 6),
            "sell_shares": round(self.sell_shares, 6),
            "net_shares": round(net_shares, 6),
            "buy_usdc": round(self.buy_usdc, 6),
            "sell_usdc": round(self.sell_usdc, 6),
            "buy_vwap": buy_vwap,
            "sell_vwap": sell_vwap,
            "min_price": self.min_price,
            "max_price": self.max_price,
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
        }


class Aggregator:
    def __init__(self) -> None:
        self.counts_by_type: Dict[str, int] = {}
        self.by_market: Dict[Tuple[str, str], TradeAggregate] = {}
        self.events_seen_total: int = 0
        self.last_event_ts: Optional[int] = None

    def update(self, event: Dict[str, Any]) -> None:
        self.events_seen_total += 1

        ts = _as_int(event.get("timestamp"))
        if ts is not None:
            self.last_event_ts = ts if self.last_event_ts is None else max(self.last_event_ts, ts)

        event_type = str(event.get("type", "UNKNOWN"))
        self.counts_by_type[event_type] = self.counts_by_type.get(event_type, 0) + 1

        if event_type != "TRADE":
            return

        condition_id = str(event.get("conditionId") or "")
        outcome = str(event.get("outcome") or "")
        slug = event.get("slug")
        title = event.get("title")

        if not condition_id or not outcome:
            return

        key = (condition_id, outcome)
        if key not in self.by_market:
            self.by_market[key] = TradeAggregate(condition_id, outcome, slug, title)
        self.by_market[key].update(event)

    def snapshot(self) -> Dict[str, Any]:
        per_market = [agg.to_snapshot() for agg in self.by_market.values()]
        return {
            "eventsSeenTotal": self.events_seen_total,
            "lastEventTs": self.last_event_ts,
            "uniqueMarketOutcomes": len(self.by_market),
            "countsByType": self.counts_by_type,
            "perMarketOutcome": per_market,
        }


def backoff_delay_seconds(attempt: int, status_code: Optional[int]) -> float:
    if status_code == 429:
        return random.uniform(10, 20)
    if status_code is not None and 500 <= status_code < 600:
        base = min(2 ** attempt, 30)
        return base + random.uniform(0, 1.5)
    base = min(1.5 ** attempt, 10)
    return base + random.uniform(0, 1.0)


def safe_fetch_activity(
    client: DataClient,
    wallet: str,
    start_ts: int,
    end_ts: int,
    limit: int,
) -> List[Dict[str, Any]]:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return client.fetch_activity(wallet, start_ts, end_ts, limit)
        except requests.HTTPError as exc:
            last_exc = exc
            status = exc.response.status_code if exc.response is not None else None
            delay = backoff_delay_seconds(attempt, status)
            logging.warning("HTTP error (status=%s). Retry %d/%d in %.1fs", status, attempt, MAX_RETRIES, delay)
            time.sleep(delay)
        except requests.RequestException as exc:
            last_exc = exc
            delay = backoff_delay_seconds(attempt, None)
            logging.warning(
                "Network error. Retry %d/%d in %.1fs (%s)", attempt, MAX_RETRIES, delay, exc.__class__.__name__
            )
            time.sleep(delay)

    logging.error("Failed to fetch activity after %d retries: %r", MAX_RETRIES, last_exc)
    return []


def write_summary(
    out_dir: str,
    address: str,
    started_at: float,
    now_ts: float,
    aggregator: Aggregator,
) -> None:
    summary = {
        "address": address,
        "startedAtUtc": utc_iso(started_at),
        "nowUtc": utc_iso(now_ts),
        **aggregator.snapshot(),
    }
    path = os.path.join(out_dir, SUMMARY_FILENAME)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Track Polymarket user activity (by address)")
    p.add_argument("--address", required=True, help="Account address to track (0x...)")
    p.add_argument("--minutes", type=int, default=60, help="Runtime in minutes")
    p.add_argument("--poll", type=int, default=POLL_SECONDS_DEFAULT, help="Polling interval (seconds)")
    p.add_argument(
        "--lookback",
        type=int,
        default=LOOKBACK_SECONDS_DEFAULT,
        help="Overlap seconds (safety buffer). Not a fixed window anymore.",
    )
    p.add_argument("--limit", type=int, default=PAGE_LIMIT_DEFAULT, help="Page limit for activity fetch")
    p.add_argument("--warmup", type=int, default=WARMUP_SECONDS_DEFAULT, help="Warmup seconds on start")
    p.add_argument("--out-dir", default="data", help="Output directory for JSON files")
    p.add_argument("--sqlite", default=None, help="Optional path for sqlite storage")
    p.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    address = normalize_address(args.address)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "polymarket-activity-tracker/1.2",
            "Accept": "application/json",
        }
    )

    data_client = DataClient(session)
    store = EventStore(args.out_dir, args.sqlite)
    aggregator = Aggregator()

    started_at = time.time()
    end_time = started_at + args.minutes * 60

    # Cursor-based polling + overlap to avoid missing events
    warmup_start = int(started_at - args.warmup)
    cursor_ts = warmup_start
    overlap = max(5, int(args.lookback))

    def process_events(events: List[Dict[str, Any]]) -> None:
        for event in events:
            dedupe_key = compute_dedupe_key(event)
            if store.seen(dedupe_key):
                continue

            store.add(event, dedupe_key)
            aggregator.update(event)

            # Status update on every newly logged trade
            if event.get("type") == "TRADE":
                log_trade(event)

    # Initial warmup fetch
    logging.info("Warmup fetch: start=%d end=%d", warmup_start, int(started_at))
    initial_events = safe_fetch_activity(data_client, address, warmup_start, int(started_at), args.limit)
    process_events(initial_events)

    if initial_events:
        max_ts = max((_as_int(e.get("timestamp")) or cursor_ts) for e in initial_events)
        cursor_ts = max(cursor_ts, max_ts)

    write_summary(args.out_dir, address, started_at, started_at, aggregator)

    # Main loop
    while time.time() < end_time:
        now = int(time.time())
        start = max(warmup_start, cursor_ts - overlap)

        logging.debug("Polling: start=%d end=%d cursor=%d overlap=%d", start, now, cursor_ts, overlap)
        events = safe_fetch_activity(data_client, address, start, now, args.limit)
        process_events(events)

        if events:
            max_ts = max((_as_int(e.get("timestamp")) or cursor_ts) for e in events)
            cursor_ts = max(cursor_ts, max_ts)

        write_summary(args.out_dir, address, started_at, time.time(), aggregator)
        time.sleep(max(1, int(args.poll)))

    write_summary(args.out_dir, address, started_at, time.time(), aggregator)
    store.close()
    logging.info("Done. Total unique events logged this run: %d", aggregator.events_seen_total)


if __name__ == "__main__":
    main()

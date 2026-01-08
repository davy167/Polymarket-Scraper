#!/usr/bin/env python3
"""Polymarket Activity Tracker."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
DATA_BASE_URL = "https://data-api.polymarket.com"

POLL_SECONDS_DEFAULT = 7
LOOKBACK_SECONDS_DEFAULT = 120
PAGE_LIMIT_DEFAULT = 200
WARMUP_SECONDS_DEFAULT = 600

RAW_EVENTS_FILENAME = "raw_events.jsonl"
SUMMARY_FILENAME = "snapshot_summary.json"


def utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def stable_field(event: Dict[str, Any], key: str) -> Optional[Any]:
    value = event.get(key)
    if value is None:
        return None
    return value


def compute_dedupe_key(event: Dict[str, Any]) -> str:
    if "id" in event and event["id"]:
        return str(event["id"])
    tx_hash = event.get("transactionHash") or event.get("transaction_hash")
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
    ]
    stable_payload = {key: stable_field(event, key) for key in stable_keys}
    payload = json.dumps(stable_payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class GammaClient:
    def __init__(self, session: requests.Session) -> None:
        self.session = session

    def resolve_wallet(self, username: str) -> str:
        url = f"{GAMMA_BASE_URL}/public-search"
        response = self.session.get(url, params={"q": username}, timeout=10)
        response.raise_for_status()
        payload = response.json()
        results = payload.get("results") if isinstance(payload, dict) else payload
        if not results:
            raise ValueError(f"No results found for username '{username}'")
        best = self._pick_best_match(results, username)
        for key in ("proxyWallet", "proxy_wallet", "wallet", "address"):
            if isinstance(best, dict) and best.get(key):
                return str(best[key])
        raise ValueError("Unable to resolve proxy wallet from Gamma API response")

    @staticmethod
    def _pick_best_match(results: Iterable[Any], username: str) -> Any:
        username_lower = username.lower()
        for item in results:
            if isinstance(item, dict):
                handle = (item.get("username") or item.get("handle") or "").lower()
                if handle == username_lower:
                    return item
        return list(results)[0]


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
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            batch = response.json()
            if not isinstance(batch, list):
                batch = batch.get("data", [])
            events.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        return events


class EventStore:
    def __init__(self, out_dir: str, sqlite_path: Optional[str] = None) -> None:
        self.out_dir = out_dir
        os.makedirs(out_dir, exist_ok=True)
        self.raw_path = os.path.join(out_dir, RAW_EVENTS_FILENAME)
        self.sqlite_path = sqlite_path
        self._seen: set[str] = set()
        self._db: Optional[sqlite3.Connection] = None
        if sqlite_path:
            self._init_db(sqlite_path)

    def _init_db(self, path: str) -> None:
        self._db = sqlite3.connect(path)
        cursor = self._db.cursor()
        cursor.execute(
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
                raw_json TEXT
            )
            """
        )
        self._db.commit()
        cursor.execute("SELECT dedupe_key FROM events")
        rows = cursor.fetchall()
        self._seen.update(row[0] for row in rows)

    def seen(self, dedupe_key: str) -> bool:
        return dedupe_key in self._seen

    def add(self, event: Dict[str, Any], dedupe_key: str) -> None:
        self._seen.add(dedupe_key)
        payload = dict(event)
        payload["__dedupe_key"] = dedupe_key
        with open(self.raw_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
        if self._db:
            cursor = self._db.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO events (
                    dedupe_key, ts, type, slug, conditionId, outcome, side, price, size, usdcSize, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dedupe_key,
                    event.get("timestamp"),
                    event.get("type"),
                    event.get("slug"),
                    event.get("conditionId"),
                    event.get("outcome"),
                    event.get("side"),
                    event.get("price"),
                    event.get("size"),
                    event.get("usdcSize"),
                    json.dumps(event, separators=(",", ":")),
                ),
            )
            self._db.commit()


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
        price = float(event.get("price")) if event.get("price") is not None else None
        size = float(event.get("size")) if event.get("size") is not None else 0.0
        usdc_size = float(event.get("usdcSize")) if event.get("usdcSize") is not None else 0.0
        timestamp = int(event.get("timestamp")) if event.get("timestamp") is not None else None
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

    def to_snapshot(self) -> Dict[str, Any]:
        net_shares = self.buy_shares - self.sell_shares
        buy_vwap = self.buy_usdc / self.buy_shares if self.buy_shares else None
        sell_vwap = self.sell_usdc / self.sell_shares if self.sell_shares else None
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

    def update(self, event: Dict[str, Any]) -> None:
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
            "countsByType": self.counts_by_type,
            "perMarketOutcome": per_market,
        }


def backoff_sleep(status_code: int, retry_state: Dict[str, int]) -> None:
    if status_code == 429:
        delay = random.uniform(10, 20)
        time.sleep(delay)
        return
    if 500 <= status_code < 600:
        retry_state["count"] += 1
        delay = min(2 ** retry_state["count"], 30)
        time.sleep(delay)


def safe_fetch_activity(
    client: DataClient,
    wallet: str,
    start_ts: int,
    end_ts: int,
    limit: int,
    retry_state: Dict[str, int],
) -> List[Dict[str, Any]]:
    try:
        events = client.fetch_activity(wallet, start_ts, end_ts, limit)
        retry_state["count"] = 0
        return events
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response else 0
        if status:
            backoff_sleep(status, retry_state)
        return []
    except requests.RequestException:
        return []


def write_summary(
    out_dir: str,
    username: str,
    wallet: str,
    started_at: float,
    now_ts: float,
    aggregator: Aggregator,
) -> None:
    summary = {
        "username": username,
        "proxyWallet": wallet,
        "startedAtUtc": utc_iso(started_at),
        "nowUtc": utc_iso(now_ts),
        **aggregator.snapshot(),
    }
    path = os.path.join(out_dir, SUMMARY_FILENAME)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Track Polymarket user activity")
    parser.add_argument("--username", required=True, help="Polymarket username to track")
    parser.add_argument("--minutes", type=int, default=60, help="Runtime in minutes")
    parser.add_argument("--poll", type=int, default=POLL_SECONDS_DEFAULT, help="Polling interval")
    parser.add_argument(
        "--lookback",
        type=int,
        default=LOOKBACK_SECONDS_DEFAULT,
        help="Lookback seconds for activity window",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=PAGE_LIMIT_DEFAULT,
        help="Page limit for activity fetch",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=WARMUP_SECONDS_DEFAULT,
        help="Warmup seconds on start",
    )
    parser.add_argument(
        "--out-dir",
        default="data",
        help="Output directory for JSON files",
    )
    parser.add_argument(
        "--sqlite",
        default=None,
        help="Optional path for sqlite storage",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = requests.Session()
    gamma = GammaClient(session)
    data_client = DataClient(session)

    wallet = gamma.resolve_wallet(args.username)
    started_at = time.time()
    warmup_start = int(started_at - args.warmup)
    store = EventStore(args.out_dir, args.sqlite)
    aggregator = Aggregator()
    retry_state = {"count": 0}

    end_time = started_at + args.minutes * 60

    def process_events(events: List[Dict[str, Any]]) -> None:
        for event in events:
            dedupe_key = compute_dedupe_key(event)
            if store.seen(dedupe_key):
                continue
            store.add(event, dedupe_key)
            aggregator.update(event)

    initial_events = safe_fetch_activity(
        data_client,
        wallet,
        warmup_start,
        int(started_at),
        args.limit,
        retry_state,
    )
    process_events(initial_events)
    write_summary(args.out_dir, args.username, wallet, started_at, started_at, aggregator)

    while time.time() < end_time:
        now = time.time()
        window_start = int(now - args.lookback)
        window_end = int(now)
        events = safe_fetch_activity(
            data_client,
            wallet,
            window_start,
            window_end,
            args.limit,
            retry_state,
        )
        process_events(events)
        write_summary(args.out_dir, args.username, wallet, started_at, now, aggregator)
        time.sleep(args.poll)

    write_summary(args.out_dir, args.username, wallet, started_at, time.time(), aggregator)


if __name__ == "__main__":
    main()

# Polymarket-Scraper

A small CLI bot that tracks a Polymarket username's activity via official APIs, deduplicates events, and emits JSON outputs you can analyze.

## Requirements

- Python 3.11+
- `pip install -r requirements.txt`

## Usage

```bash
python tracker.py --username <PolymarketUsername> --minutes 60 --poll 7
```

### Common options

- `--poll`: polling interval in seconds (default: 7)
- `--lookback`: lookback window for each poll in seconds (default: 120)
- `--limit`: page size for each API request (default: 200)
- `--warmup`: warmup window on start in seconds (default: 600)
- `--out-dir`: output directory for JSON files (default: `data`)
- `--sqlite`: optional sqlite path to persist events (e.g. `data/events.sqlite`)

Example:

```bash
python tracker.py --username Account88888 --minutes 60 --poll 7 --lookback 120 --warmup 600 --out-dir data --sqlite data/events.sqlite
```

## Output files

By default, the tracker writes to the `data/` directory:

- `raw_events.jsonl`: one JSON object per line containing the raw API event plus a `__dedupe_key` field.
- `snapshot_summary.json`: aggregated snapshot updated every poll with counts by event type and per-market outcome stats.

If `--sqlite` is supplied, a sqlite database is created (or reused) with an `events` table keyed by the dedupe key.

"""Restartable, batched SQLite sink for Redis Stream ticks.

The event rows and cursor commit in one SQLite transaction. Replaying a batch
after a failure is safe because (symbol, stream_id) is unique.
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path

import redis

from .store import TickStore


class SQLiteTickSink:
    def __init__(self, path: str | Path, store: TickStore) -> None:
        self.store = store
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS ticks (
                symbol TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                price TEXT NOT NULL,
                source_ts_ms INTEGER NOT NULL,
                PRIMARY KEY (symbol, stream_id),
                UNIQUE (symbol, sequence)
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                symbol TEXT PRIMARY KEY,
                stream_id TEXT NOT NULL
            );
        """)

    def close(self) -> None:
        self.db.close()

    def cursor(self, symbol: str) -> str:
        row = self.db.execute(
            "SELECT stream_id FROM checkpoints WHERE symbol = ?", (symbol,)
        ).fetchone()
        return row[0] if row else "0-0"

    def transfer_batch(self, symbol: str, limit: int = 100) -> int:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        ticks = self.store.replay(symbol, after=self.cursor(symbol), limit=limit)
        if not ticks:
            return 0
        with self.db:
            self.db.executemany(
                """INSERT OR IGNORE INTO ticks
                   (symbol, stream_id, sequence, event_id, price, source_ts_ms)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    (
                        t.symbol,
                        t.stream_id,
                        t.sequence,
                        t.event_id,
                        t.price,
                        t.source_ts_ms,
                    )
                    for t in ticks
                ],
            )
            self.db.execute(
                """INSERT INTO checkpoints(symbol, stream_id) VALUES (?, ?)
                   ON CONFLICT(symbol) DO UPDATE SET stream_id=excluded.stream_id""",
                (symbol, ticks[-1].stream_id),
            )
        return len(ticks)


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy one symbol into SQLite")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--sqlite", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--poll-seconds", type=float, default=0.2)
    args = parser.parse_args()
    if args.poll_seconds < 0:
        parser.error("--poll-seconds must be nonnegative")
    sink = SQLiteTickSink(args.sqlite, TickStore(redis.Redis.from_url(args.redis_url)))
    try:
        while True:
            try:
                count = sink.transfer_batch(args.symbol.upper(), args.batch_size)
            except (redis.RedisError, sqlite3.Error) as error:
                print(f"sink retrying after error: {error}", flush=True)
                time.sleep(max(args.poll_seconds, 0.2))
                continue
            if count == 0:
                time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        pass
    finally:
        sink.close()


if __name__ == "__main__":
    main()

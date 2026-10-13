"""Repeatable local publish benchmark; not a production performance claim."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import platform
import statistics
import time
import uuid

import redis

from markettape.store import TickStore


def percentile(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--events", type=int, default=5000)
    parser.add_argument("--workers", type=int, default=16)
    args = parser.parse_args()
    if args.events < 1 or args.workers < 1:
        parser.error("events and workers must be positive")

    client = redis.Redis.from_url(args.redis_url)
    store = TickStore(client)
    symbol = "BENCH" + uuid.uuid4().hex[:8].upper()

    def publish(index: int) -> tuple[float, int]:
        started = time.perf_counter_ns()
        tick, duplicate = store.publish(symbol, f"event-{index}", "120.25", index)
        assert not duplicate
        return (time.perf_counter_ns() - started) / 1_000_000, tick.sequence

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(publish, range(args.events)))
    wall_s = time.perf_counter() - started
    latencies = [latency for latency, _ in results]
    sequences = sorted(sequence for _, sequence in results)
    assert sequences == list(range(1, args.events + 1))
    redis_info = client.info("server")
    print(
        json.dumps(
            {
                "events": args.events,
                "workers": args.workers,
                "symbol": symbol,
                "throughput_events_per_s": round(args.events / wall_s, 1),
                "wall_s": round(wall_s, 3),
                "latency_ms": {
                    "p50": round(statistics.median(latencies), 3),
                    "p95": round(percentile(latencies, 0.95), 3),
                    "p99": round(percentile(latencies, 0.99), 3),
                },
                "python": platform.python_version(),
                "machine": platform.machine(),
                "redis": redis_info["redis_version"],
                "note": "Synthetic local ingest, one Redis instance; durability depends on Redis persistence configuration.",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

# MarketTape

MarketTape is a small synthetic market-data stream. It is an engineering project,
not a real market-data feed. It accepts price ticks, assigns a gap-free sequence
per symbol, rejects duplicate event IDs, and offers HTTP/WebSocket replay from a
Redis Stream cursor.

## Invariants

- A Lua script performs duplicate lookup, sequence allocation, and `XADD` in
  one Redis operation. Concurrent writers for one symbol receive a total order.
- Reusing an event ID returns the original stream position without appending.
- Each WebSocket reads at most 100 entries before sending them. A slow client
  pauses its own read loop; it can resume from its last cursor because entries
  stay in Redis. No per-client unbounded queue is held by the application.
- Redis AOF is needed for replay after a Redis crash. The tests use
  `appendfsync always`; a different persistence policy changes durability.
- A separate SQLite sink commits event rows and its replay cursor in the same
  transaction. If storage fails or the process restarts, it retries from the
  last committed cursor. The ingest and WebSocket paths do not wait on SQLite.
- Streams are not trimmed. Persistent Redis storage can grow without bound;
  operators must provision and monitor disk until a safe retention policy is
  added. This is a single-node demonstration, not an HA market-data service.

## Run

```sh
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
redis-server --appendonly yes --appendfsync always
MARKETTAPE_REDIS_URL=redis://127.0.0.1:6379/0 .venv/bin/uvicorn markettape.app:app
```

Send a synthetic tick:

```sh
curl -X POST http://127.0.0.1:8000/v1/ticks \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"AAPL","event_id":"demo-1","price":"120.25","source_ts_ms":1}'
```

Replay at `/v1/ticks/AAPL?after=0-0`; connect a WebSocket to
`/v1/stream/AAPL?after=0-0`.

Run the independent storage sink for a symbol:

```sh
PYTHONPATH=. .venv/bin/python -m markettape.sink --symbol AAPL \
  --sqlite ./ticks.sqlite --batch-size 100
```

The SQLite file can be deleted and rebuilt from Redis while the stream is
retained. Run a separate sink process per symbol. A sink failure only delays
the SQLite copy; Redis remains the source of truth for the API.

## Tests

```sh
.venv/bin/pytest -q
```

The suite starts isolated Redis instances and checks 500 writes across 24 threads,
duplicate handling, cursor pagination, WebSocket replay, and unclean Redis
restart with AOF recovery. Sink tests cover batch restart, fault rollback,
and retry without duplicate rows.

## Local benchmark

Run with a disposable Redis instance (the symbol is unique for each run):

```sh
PYTHONPATH=. .venv/bin/python benchmarks/publish.py \
  --redis-url redis://127.0.0.1:6379/0 --events 5000 --workers 16
```

Two runs on an arm64 Mac with Python 3.11.15 and Redis 8.6.2, AOF `everysec`,
measured 11,627–12,319 synthetic publishes/s. The p95 client-side publish
latency was 2.07–2.90 ms. This is a local single-Redis microbenchmark, not a
production or multi-region result. No stock-exchange feed is involved.

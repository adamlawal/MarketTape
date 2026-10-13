from __future__ import annotations

import concurrent.futures
import shutil
import sqlite3
import socket
import subprocess
import time

import pytest
import redis
from fastapi.testclient import TestClient

from markettape.app import create_app
from markettape.store import TickStore
from markettape.sink import SQLiteTickSink


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def start_redis(port: int, directory: str) -> subprocess.Popen[bytes]:
    process = subprocess.Popen(
        [
            "redis-server", "--bind", "127.0.0.1", "--port", str(port),
            "--save", "", "--appendonly", "yes", "--appendfsync", "always",
            "--dir", directory,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError("Redis exited before startup")
        try:
            redis.Redis(host="127.0.0.1", port=port, socket_timeout=0.1).ping()
            return process
        except redis.RedisError:
            time.sleep(0.02)
    process.terminate()
    raise RuntimeError("Redis did not become ready")


@pytest.fixture
def redis_service(tmp_path):
    if not shutil.which("redis-server"):
        pytest.skip("redis-server is required")
    port = free_port()
    process = start_redis(port, str(tmp_path))
    url = f"redis://127.0.0.1:{port}/0"
    try:
        yield url, port, process
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)


def test_concurrent_writers_produce_gap_free_per_symbol_order(redis_service):
    url, _, _ = redis_service
    store = TickStore(redis.Redis.from_url(url))
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
        published = list(pool.map(
            lambda n: store.publish("BBG", f"event-{n}", str(100 + n), n),
            range(500),
        ))
    assert all(not duplicate for _, duplicate in published)
    ticks = store.replay("BBG", limit=1000)
    assert [tick.sequence for tick in ticks] == list(range(1, 501))
    assert len({tick.stream_id for tick in ticks}) == 500


def test_idempotency_and_cursor_replay(redis_service):
    url, _, _ = redis_service
    store = TickStore(redis.Redis.from_url(url))
    first, duplicate = store.publish("META", "same-event", "100.25", 1000)
    again, duplicate_again = store.publish("META", "same-event", "100.25", 1000)
    assert duplicate is False
    assert duplicate_again is True
    assert again.stream_id == first.stream_id
    for number in range(1, 201):
        store.publish("META", f"event-{number}", str(number), number)
    page1 = store.replay("META", limit=100)
    page2 = store.replay("META", after=page1[-1].stream_id, limit=100)
    page3 = store.replay("META", after=page2[-1].stream_id, limit=100)
    assert len(page1) + len(page2) + len(page3) == 201
    assert page3[0].sequence == 201


def test_http_ingest_and_websocket_replay(redis_service):
    url, _, _ = redis_service
    with TestClient(create_app(url)) as client:
        for number in range(3):
            response = client.post("/v1/ticks", json={
                "symbol": "AAPL", "event_id": f"tick-{number}",
                "price": f"120.{number}", "source_ts_ms": number,
            })
            assert response.status_code == 200
        history = client.get("/v1/ticks/AAPL?limit=2").json()["ticks"]
        assert [tick["sequence"] for tick in history] == [1, 2]
        with client.websocket_connect(
            f"/v1/stream/AAPL?after={history[-1]['stream_id']}"
        ) as websocket:
            assert websocket.receive_json()["sequence"] == 3
            client.post("/v1/ticks", json={
                "symbol": "AAPL", "event_id": "tick-3", "price": "120.3",
                "source_ts_ms": 3,
            })
            assert websocket.receive_json()["sequence"] == 4


def test_aof_replay_after_unclean_redis_restart(redis_service, tmp_path):
    url, port, process = redis_service
    store = TickStore(redis.Redis.from_url(url))
    for number in range(50):
        store.publish("MSFT", f"event-{number}", str(number + 1), number)
    last_cursor = store.replay("MSFT", limit=1)[-1].stream_id
    process.kill()
    process.wait(timeout=5)
    replacement = start_redis(port, str(tmp_path))
    try:
        recovered = TickStore(redis.Redis.from_url(url)).replay("MSFT", after=last_cursor)
        assert [tick.sequence for tick in recovered] == list(range(2, 51))
    finally:
        replacement.terminate()
        replacement.wait(timeout=5)


def test_sink_restarts_from_committed_cursor_without_duplicates(redis_service, tmp_path):
    url, _, _ = redis_service
    store = TickStore(redis.Redis.from_url(url))
    path = tmp_path / "ticks.sqlite"
    for number in range(250):
        store.publish("BBG", f"tick-{number}", str(number + 1), number)
    sink = SQLiteTickSink(path, store)
    assert sink.transfer_batch("BBG", 100) == 100
    first_cursor = sink.cursor("BBG")
    sink.close()
    restarted = SQLiteTickSink(path, store)
    assert restarted.cursor("BBG") == first_cursor
    assert restarted.transfer_batch("BBG", 100) == 100
    assert restarted.transfer_batch("BBG", 100) == 50
    assert restarted.transfer_batch("BBG", 100) == 0
    sequences = [row[0] for row in restarted.db.execute(
        "SELECT sequence FROM ticks WHERE symbol='BBG' ORDER BY sequence"
    )]
    assert sequences == list(range(1, 251))
    restarted.close()


def test_sink_failed_transaction_keeps_cursor_and_retries(redis_service, tmp_path):
    url, _, _ = redis_service
    store = TickStore(redis.Redis.from_url(url))
    path = tmp_path / "ticks.sqlite"
    for number in range(3):
        store.publish("META", f"tick-{number}", str(number + 1), number)
    sink = SQLiteTickSink(path, store)
    sink.db.execute("""CREATE TRIGGER fail_second BEFORE INSERT ON ticks
        WHEN NEW.sequence = 2 BEGIN SELECT RAISE(ABORT, 'simulated sink fault'); END""")
    with pytest.raises(sqlite3.IntegrityError):
        sink.transfer_batch("META", 3)
    assert sink.cursor("META") == "0-0"
    assert sink.db.execute("SELECT count(*) FROM ticks").fetchone()[0] == 0
    sink.db.execute("DROP TRIGGER fail_second")
    assert sink.transfer_batch("META", 3) == 3
    assert sink.db.execute("SELECT count(*) FROM ticks").fetchone()[0] == 3
    sink.close()

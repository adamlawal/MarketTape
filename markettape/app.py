"""HTTP ingestion and replayable WebSocket fan-out for synthetic price ticks."""

from __future__ import annotations

import os
import re

import redis
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from .store import TickStore

SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9._-]{0,15}$")
CURSOR_RE = re.compile(r"^\d+-\d+$")


class PublishRequest(BaseModel):
    symbol: str
    event_id: str = Field(min_length=1, max_length=64)
    price: str = Field(pattern=r"^\d+(\.\d{1,8})?$")
    source_ts_ms: int = Field(ge=0)


def _symbol(value: str) -> str:
    symbol = value.upper()
    if not SYMBOL_RE.fullmatch(symbol):
        raise HTTPException(422, "Invalid symbol")
    return symbol


def _cursor(value: str) -> str:
    if not CURSOR_RE.fullmatch(value):
        raise HTTPException(422, "Invalid Redis stream cursor")
    return value


def create_app(redis_url: str | None = None) -> FastAPI:
    url = redis_url or os.environ.get(
        "MARKETTAPE_REDIS_URL", "redis://127.0.0.1:6379/0"
    )
    sync_client = redis.Redis.from_url(url)
    store = TickStore(sync_client)
    app = FastAPI(title="MarketTape", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        try:
            sync_client.ping()
        except redis.RedisError:
            raise HTTPException(503, "Redis unavailable")
        return {"status": "ok"}

    @app.post("/v1/ticks")
    def publish(request: PublishRequest) -> dict[str, object]:
        symbol = _symbol(request.symbol)
        try:
            tick, duplicate = store.publish(
                symbol, request.event_id, request.price, request.source_ts_ms
            )
        except redis.RedisError:
            raise HTTPException(503, "Redis unavailable")
        return {"tick": tick.as_dict(), "duplicate": duplicate}

    @app.get("/v1/ticks/{symbol}")
    def replay(symbol: str, after: str = "0-0", limit: int = 100) -> dict[str, object]:
        symbol = _symbol(symbol)
        after = _cursor(after)
        if not 1 <= limit <= 1000:
            raise HTTPException(422, "limit must be between 1 and 1000")
        try:
            ticks = store.replay(symbol, after, limit)
        except redis.RedisError:
            raise HTTPException(503, "Redis unavailable")
        return {"ticks": [tick.as_dict() for tick in ticks]}

    @app.websocket("/v1/stream/{symbol}")
    async def stream(websocket: WebSocket, symbol: str, after: str = "0-0") -> None:
        try:
            symbol = _symbol(symbol)
            after = _cursor(after)
        except HTTPException:
            await websocket.close(code=1008)
            return
        await websocket.accept()
        stream_key = TickStore.keys(symbol)[2]
        client = aioredis.Redis.from_url(url)
        cursor = after
        try:
            while True:
                # One bounded batch at a time. A slow client stops the read loop,
                # while Redis retains unread events for cursor-based replay.
                batches = await client.xread(
                    {stream_key: cursor}, count=100, block=1000
                )
                for _, events in batches:
                    for item in events:
                        tick = TickStore.decode(symbol, item)
                        await websocket.send_json(tick.as_dict())
                        cursor = tick.stream_id
        except WebSocketDisconnect:
            pass
        except redis.RedisError:
            await websocket.close(
                code=1011, reason="Redis unavailable; reconnect with last cursor"
            )
        finally:
            await client.aclose()

    return app


app = create_app()

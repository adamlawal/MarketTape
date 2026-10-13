"""Atomic per-symbol sequencing, idempotency, and replay on Redis Streams."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import redis


PUBLISH_LUA = """
local previous = redis.call('HGET', KEYS[2], ARGV[1])
if previous then
  return {previous, 1, 0}
end
local seq = redis.call('INCR', KEYS[1])
local stream_id = redis.call('XADD', KEYS[3], '*',
  'seq', seq,
  'event_id', ARGV[1],
  'price', ARGV[2],
  'source_ts_ms', ARGV[3])
redis.call('HSET', KEYS[2], ARGV[1], stream_id)
return {stream_id, 0, seq}
"""


@dataclass(frozen=True)
class Tick:
    symbol: str
    stream_id: str
    sequence: int
    event_id: str
    price: str
    source_ts_ms: int

    def as_dict(self) -> dict[str, str | int]:
        return {
            "symbol": self.symbol,
            "stream_id": self.stream_id,
            "sequence": self.sequence,
            "event_id": self.event_id,
            "price": self.price,
            "source_ts_ms": self.source_ts_ms,
        }


class TickStore:
    def __init__(self, client: redis.Redis) -> None:
        self.client = client
        self._publish = client.register_script(PUBLISH_LUA)

    @staticmethod
    def keys(symbol: str) -> tuple[str, str, str]:
        # The hash tag keeps all three keys in one Redis Cluster slot.
        prefix = f"markettape:{{{symbol}}}"
        return f"{prefix}:seq", f"{prefix}:dedup", f"{prefix}:stream"

    def publish(self, symbol: str, event_id: str, price: str, source_ts_ms: int) -> tuple[Tick, bool]:
        seq_key, dedup_key, stream_key = self.keys(symbol)
        raw_id, duplicate, sequence = self._publish(
            keys=[seq_key, dedup_key, stream_key],
            args=[event_id, price, source_ts_ms],
        )
        stream_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
        if duplicate:
            tick = self._one(symbol, stream_id)
            if tick is None:
                raise RuntimeError("Original stream entry was not found")
            return tick, True
        return Tick(symbol, stream_id, int(sequence), event_id, price, source_ts_ms), False

    def _one(self, symbol: str, stream_id: str) -> Tick | None:
        stream_key = self.keys(symbol)[2]
        values = self.client.xrange(stream_key, min=stream_id, max=stream_id, count=1)
        if not values:
            return None
        return self.decode(symbol, values[0])

    def replay(self, symbol: str, after: str = "0-0", limit: int = 100) -> list[Tick]:
        stream_key = self.keys(symbol)[2]
        values = self.client.xrange(stream_key, min=f"({after}", max="+", count=limit)
        return [self.decode(symbol, item) for item in values]

    @staticmethod
    def decode(symbol: str, item: tuple[Any, dict[Any, Any]]) -> Tick:
        raw_id, raw_fields = item
        stream_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
        fields = {
            (k.decode() if isinstance(k, bytes) else str(k)):
            (v.decode() if isinstance(v, bytes) else str(v))
            for k, v in raw_fields.items()
        }
        return Tick(
            symbol=symbol,
            stream_id=stream_id,
            sequence=int(fields["seq"]),
            event_id=fields["event_id"],
            price=fields["price"],
            source_ts_ms=int(fields["source_ts_ms"]),
        )

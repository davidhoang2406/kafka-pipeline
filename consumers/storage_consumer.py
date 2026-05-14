# Subscribes to Kafka price topics and persists messages to MinIO as partitioned Avro files.
# Partition layout: {event_type}/symbol={symbol}/year={year}/month={month}/day={day}/part-{ts}.avro
# Batches writes (up to 500 rows or 30 s) to keep file sizes reasonable.
import logging
import os
import time
from collections import defaultdict

import fastavro
from dotenv import load_dotenv

from consumers.base_consumer import BaseConsumer
from model.minio_store import MinioStore

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPICS         = ["stock.price.realtime", "crypto.price.realtime"]
GROUP_ID       = "storage"
BATCH_SIZE     = 500   # flush after this many rows total
FLUSH_INTERVAL = 30    # also flush after this many seconds even if batch isn't full

# ── Avro schemas ──────────────────────────────────────────────────────────────

_SCHEMAS = {
    "price.snapshot": fastavro.parse_schema({
        "type": "record", "name": "PriceSnapshot",
        "fields": [
            {"name": "time",       "type": "string"},
            {"name": "symbol",     "type": "string"},
            {"name": "exchange",   "type": "string"},
            {"name": "price",      "type": "double"},
            {"name": "change",     "type": "double"},
            {"name": "pct_change", "type": "double"},
            {"name": "volume",     "type": "long"},
            {"name": "bid",        "type": "double"},
            {"name": "ask",        "type": "double"},
        ],
    }),
}

# ── Row extractors ────────────────────────────────────────────────────────────

def _f(v) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _i(v) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


_EXTRACTORS = {
    "price.snapshot": lambda m: {
        "time":       m["timestamp"],
        "symbol":     m["symbol"],
        "exchange":   m.get("exchange", ""),
        "price":      _f(m["payload"].get("price")),
        "change":     _f(m["payload"].get("change")),
        "pct_change": _f(m["payload"].get("pct_change")),
        "volume":     _i(m["payload"].get("volume")),
        "bid":        _f(m["payload"].get("bid")),
        "ask":        _f(m["payload"].get("ask")),
    },
}


# ── Storage buffer ────────────────────────────────────────────────────────────

def _date_parts(date: str) -> tuple[str, str, str]:
    """Split 'YYYY-MM-DD' into (year, month, day). Returns 'unknown' on bad input."""
    if len(date) == 10:
        return date[:4], date[5:7], date[8:10]
    return "unknown", "unknown", "unknown"


class _Buffer:
    """
    Accumulates rows keyed by (event_type, symbol, year, month, day).
    On flush, writes one Avro file per key to MinIO.
    """

    def __init__(self, store: MinioStore):
        self._store      = store
        self._rows: dict[tuple, list] = defaultdict(list)
        self._last_flush = time.monotonic()

    def add(self, msg: dict) -> None:
        event_type = msg.get("event_type")
        if event_type not in _EXTRACTORS:
            return
        row    = _EXTRACTORS[event_type](msg)
        symbol = msg.get("symbol", "UNKNOWN")
        date   = msg.get("timestamp", "")[:10] or "unknown"
        year, month, day = _date_parts(date)
        self._rows[(event_type, symbol, year, month, day)].append(row)

    def total_rows(self) -> int:
        return sum(len(v) for v in self._rows.values())

    def should_flush(self) -> bool:
        return (
            self.total_rows() >= BATCH_SIZE
            or time.monotonic() - self._last_flush >= FLUSH_INTERVAL
        )

    def flush(self) -> None:
        if not self._rows:
            self._last_flush = time.monotonic()
            return

        ts_ms = int(time.time() * 1000)
        for (event_type, symbol, year, month, day), rows in self._rows.items():
            key    = (f"{event_type}/symbol={symbol}"
                      f"/year={year}/month={month}/day={day}/part-{ts_ms}.avro")
            schema = _SCHEMAS[event_type]
            self._store.write_avro(key, schema, rows)

        self._rows.clear()
        self._last_flush = time.monotonic()


# ── Entry point ───────────────────────────────────────────────────────────────

def run() -> None:
    store = MinioStore(os.getenv("MINIO_BUCKET", "market-data"))
    buf   = _Buffer(store)

    log.info("StorageConsumer started | bucket=%s | topics=%s", store.bucket, TOPICS)

    with BaseConsumer(TOPICS, group_id=GROUP_ID) as consumer:
        while True:
            for record in consumer.poll(timeout_ms=1000):
                buf.add(record.value)
            if buf.should_flush():
                buf.flush()

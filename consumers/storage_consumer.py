# Subscribes to all five Kafka topics (stock + crypto) and persists messages to MinIO
# as partitioned Parquet files (Snappy-compressed).
# Partition layout: s3://market-data/{event_type}/symbol={symbol}/date={date}/part-{ts}.parquet
# Batches writes (up to 500 rows or 30 s) to keep file sizes reasonable.
import io
import logging
import os
import time
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

from minio import Minio

from consumers.base_consumer import BaseConsumer

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPICS         = ["stock.price.realtime", "stock.ohlcv.daily", "stock.financials",
                  "crypto.price.realtime", "crypto.ohlcv.daily"]
GROUP_ID       = "storage"
BATCH_SIZE     = 500   # flush after this many rows total
FLUSH_INTERVAL = 30    # also flush after this many seconds even if batch isn't full

# ── PyArrow schemas ───────────────────────────────────────────────────────────

_SCHEMAS = {
    "price.snapshot": pa.schema([
        ("time",       pa.string()),
        ("symbol",     pa.string()),
        ("exchange",   pa.string()),
        ("price",      pa.float64()),
        ("change",     pa.float64()),
        ("pct_change", pa.float64()),
        ("volume",     pa.int64()),
        ("bid",        pa.float64()),
        ("ask",        pa.float64()),
    ]),
    "ohlcv.bar": pa.schema([
        ("time",     pa.string()),
        ("symbol",   pa.string()),
        ("exchange", pa.string()),
        ("open",     pa.float64()),
        ("high",     pa.float64()),
        ("low",      pa.float64()),
        ("close",    pa.float64()),
        ("volume",   pa.int64()),
    ]),
    "financials.report": pa.schema([
        ("report_date",  pa.string()),
        ("symbol",       pa.string()),
        ("period",       pa.string()),
        ("revenue",      pa.float64()),
        ("net_income",   pa.float64()),
        ("total_assets", pa.float64()),
        ("total_debt",   pa.float64()),
        ("eps",          pa.float64()),
    ]),
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
    "ohlcv.bar": lambda m: {
        "time":     m["timestamp"],
        "symbol":   m["symbol"],
        "exchange": m.get("exchange", ""),
        "open":     _f(m["payload"].get("open")),
        "high":     _f(m["payload"].get("high")),
        "low":      _f(m["payload"].get("low")),
        "close":    _f(m["payload"].get("close")),
        "volume":   _i(m["payload"].get("volume")),
    },
    "financials.report": lambda m: {
        "report_date":  m["payload"].get("report_date", m["timestamp"][:10]),
        "symbol":       m["symbol"],
        "period":       m["payload"].get("period", ""),
        "revenue":      _f(m["payload"].get("revenue")),
        "net_income":   _f(m["payload"].get("net_income")),
        "total_assets": _f(m["payload"].get("total_assets")),
        "total_debt":   _f(m["payload"].get("total_debt")),
        "eps":          _f(m["payload"].get("eps")),
    },
}


# ── Storage buffer ────────────────────────────────────────────────────────────

class _Buffer:
    """
    Accumulates rows keyed by (event_type, symbol, date).
    On flush, writes one Parquet file per key to MinIO.
    """

    def __init__(self, client: Minio, bucket: str):
        self._client     = client
        self._bucket     = bucket
        self._rows: dict[tuple, list] = defaultdict(list)
        self._last_flush = time.monotonic()

    def add(self, msg: dict) -> None:
        event_type = msg.get("event_type")
        if event_type not in _EXTRACTORS:
            return
        row    = _EXTRACTORS[event_type](msg)
        symbol = msg.get("symbol", "UNKNOWN")
        date   = msg.get("timestamp", "")[:10] or "unknown"
        self._rows[(event_type, symbol, date)].append(row)

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
        for (event_type, symbol, date), rows in self._rows.items():
            key    = f"{event_type}/symbol={symbol}/date={date}/part-{ts_ms}.parquet"
            schema = _SCHEMAS[event_type]
            table  = pa.Table.from_pylist(rows, schema=schema)
            buf    = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            data   = buf.getvalue()
            self._client.put_object(
                self._bucket, key, io.BytesIO(data), len(data),
                content_type="application/octet-stream",
            )
            log.info("wrote %3d rows → s3://%s/%s", len(rows), self._bucket, key)

        self._rows.clear()
        self._last_flush = time.monotonic()


# ── Entry point ───────────────────────────────────────────────────────────────

def _make_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    secure   = endpoint.startswith("https://")
    host     = endpoint.split("://", 1)[-1]
    return Minio(
        host,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=secure,
    )


def run() -> None:
    bucket = os.getenv("MINIO_BUCKET", "market-data")
    buf    = _Buffer(_make_client(), bucket)

    log.info("StorageConsumer started | bucket=%s | topics=%s", bucket, TOPICS)

    with BaseConsumer(TOPICS, group_id=GROUP_ID) as consumer:
        while True:
            for record in consumer.poll(timeout_ms=1000):
                buf.add(record.value)
            if buf.should_flush():
                buf.flush()

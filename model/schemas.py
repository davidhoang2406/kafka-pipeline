import fastavro
import pyarrow as pa

# ── Avro (streaming → StorageConsumer → MinIO market-data) ────────────────────

PRICE_SNAPSHOT_AVRO_SCHEMA = fastavro.parse_schema({
    "type": "record", "name": "PriceSnapshot",
    "fields": [
        {"name": "time",       "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "symbol",     "type": "string"},
        {"name": "exchange",   "type": "string"},
        {"name": "price",      "type": "double"},
        {"name": "change",     "type": "double"},
        {"name": "pct_change", "type": "double"},
        {"name": "volume",     "type": "long"},
        {"name": "bid",        "type": "double"},
        {"name": "ask",        "type": "double"},
    ],
})

# ── Parquet (batch ingest → MinIO market-analysis) ─────────────────────────────

OHLCV_BAR_SCHEMA = pa.schema([
    pa.field("time",     pa.timestamp("us", tz="UTC")),
    pa.field("symbol",   pa.string()),
    pa.field("exchange", pa.string()),
    pa.field("open",     pa.float64()),
    pa.field("high",     pa.float64()),
    pa.field("low",      pa.float64()),
    pa.field("close",    pa.float64()),
    pa.field("volume",   pa.int64()),
])

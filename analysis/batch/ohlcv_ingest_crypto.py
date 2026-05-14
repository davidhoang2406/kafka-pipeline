# Batch ingest for crypto OHLCV bars via CCXT (default: Binance).
# Bars are written directly to MinIO as Avro (bypasses Kafka entirely).
# Intended to run once per day (cron or manual trigger).
import io
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import ccxt
import fastavro
from dotenv import load_dotenv
from minio import Minio

from producers.utils import coerce_float, coerce_int, load_json_config

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CONFIG = Path(__file__).parent.parent.parent / "config" / "crypto.json"

_OHLCV_SCHEMA = fastavro.parse_schema({
    "type": "record", "name": "OhlcvBar",
    "fields": [
        {"name": "time",     "type": "string"},
        {"name": "symbol",   "type": "string"},
        {"name": "exchange", "type": "string"},
        {"name": "open",     "type": "double"},
        {"name": "high",     "type": "double"},
        {"name": "low",      "type": "double"},
        {"name": "close",    "type": "double"},
        {"name": "volume",   "type": "long"},
    ],
})


def _make_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    secure   = endpoint.startswith("https://")
    host     = endpoint.split("://", 1)[-1]
    return Minio(
        host,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=secure,
    )


def _write_ohlcv_to_minio(client: Minio, bucket: str, rows: list[dict]) -> None:
    if not rows:
        return
    ts_ms    = int(time.time() * 1000)
    symbol   = rows[0]["symbol"].replace("/", "-")
    date_str = rows[0]["time"][:10]
    year, month, day = date_str[:4], date_str[5:7], date_str[8:10]
    key = (f"ohlcv.bar/symbol={symbol}"
           f"/year={year}/month={month}/day={day}/part-{ts_ms}.avro")

    buf = io.BytesIO()
    fastavro.writer(buf, _OHLCV_SCHEMA, rows, codec="deflate")
    data = buf.getvalue()
    client.put_object(
        bucket, key, io.BytesIO(data), len(data), content_type="avro/binary",
    )
    log.info("wrote %d rows → s3://%s/%s", len(rows), bucket, key)


def _ingest_ohlcv(
    client: Minio,
    bucket: str,
    exchange_client,
    symbol: str,
    exchange_id: str,
    lookback: int,
) -> int:
    bars = exchange_client.fetch_ohlcv(symbol, timeframe="1d", limit=lookback)
    if not bars:
        log.warning("%s: fetch_ohlcv returned empty", symbol)
        return 0

    rows = []
    for bar in bars:
        ts_ms, open_, high, low, close, volume = bar
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
        rows.append({
            "time":     ts,
            "symbol":   symbol,
            "exchange": exchange_id.upper(),
            "open":     coerce_float(open_),
            "high":     coerce_float(high),
            "low":      coerce_float(low),
            "close":    coerce_float(close),
            "volume":   coerce_int(volume),
        })

    _write_ohlcv_to_minio(client, bucket, rows)
    return len(rows)


def run() -> None:
    config      = load_json_config(CONFIG)
    exchange_id: str = config["exchange"]
    symbols: list    = config["symbols"]
    lookback: int    = config.get("ohlcv_lookback_days", 1)
    bucket           = os.getenv("MINIO_BUCKET", "market-data")

    exchange_client = getattr(ccxt, exchange_id)()
    minio_client    = _make_minio_client()

    log.info(
        "Crypto OHLCV batch ingest | exchange=%s | symbols=%s | lookback=%d days",
        exchange_id, symbols, lookback,
    )

    total = 0
    for symbol in symbols:
        try:
            n = _ingest_ohlcv(minio_client, bucket, exchange_client, symbol, exchange_id, lookback)
            log.info("%s: %d OHLCV bars → MinIO", symbol, n)
            total += n
        except Exception:
            log.exception("%s: OHLCV ingest failed", symbol)

    log.info("Done | %d crypto OHLCV bars → MinIO", total)

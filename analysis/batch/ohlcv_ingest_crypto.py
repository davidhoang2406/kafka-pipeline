# Batch ingest for crypto OHLCV bars via CCXT (default: Binance).
# Bars are written directly to MinIO (market-analysis bucket) as Snappy-compressed Parquet.
# Intended to run once per day (cron or manual trigger).
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import ccxt
from dotenv import load_dotenv

from model.minio_store import MinioStore
from model.schemas import OHLCV_BAR_SCHEMA
from producers.utils import coerce_float, coerce_int, load_json_config

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CONFIG = Path(__file__).parent.parent.parent / "config" / "crypto.json"


def _ingest_ohlcv(
    store: MinioStore,
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

    store.write_partitioned_parquet("ohlcv.bar", symbol, rows, OHLCV_BAR_SCHEMA)
    return len(rows)


def run() -> None:
    config      = load_json_config(CONFIG)
    exchange_id: str = config["exchange"]
    symbols: list    = config["symbols"]
    lookback: int    = config.get("ohlcv_lookback_days", 1)
    store            = MinioStore(os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis"))

    exchange_client = getattr(ccxt, exchange_id)()

    log.info("Crypto OHLCV batch ingest | exchange=%s | symbols=%s | lookback=%d days | bucket=%s",
             exchange_id, symbols, lookback, store.bucket)

    total = 0
    for symbol in symbols:
        try:
            n = _ingest_ohlcv(store, exchange_client, symbol, exchange_id, lookback)
            log.info("%s: %d OHLCV bars → MinIO", symbol, n)
            total += n
        except Exception:
            log.exception("%s: OHLCV ingest failed", symbol)

    log.info("Done | %d crypto OHLCV bars → s3://%s", total, store.bucket)

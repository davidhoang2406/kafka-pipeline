# Batch ingest for Vietnamese stock OHLCV bars.
# Bars are written directly to MinIO (market-analysis bucket) as Snappy-compressed Parquet.
# Intended to run once per day (cron or manual trigger).
import logging
import os
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from vnstock import Quote

from model.minio_store import MinioStore
from model.schemas import OHLCV_BAR_SCHEMA
from producers.utils import coerce_float, coerce_int, load_json_config, to_ts

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CONFIG        = Path(__file__).parent.parent.parent / "config" / "symbols.json"
LOOKBACK_DAYS = 1


def _ingest_ohlcv(store: MinioStore, symbol: str, exchange: str, start: str, end: str) -> int:
    df = Quote(symbol=symbol, source="VCI").history(start=start, end=end)
    if df is None or df.empty:
        log.warning("%s: OHLCV returned empty", symbol)
        return 0

    rows = []
    for _, row in df.iterrows():
        r = row.to_dict()
        trading_date = r.get("time") or r.get("tradingDate") or r.get("date")
        if trading_date is None:
            continue
        rows.append({
            "time":     to_ts(trading_date),
            "symbol":   symbol,
            "exchange": exchange,
            "open":     coerce_float(r.get("open")),
            "high":     coerce_float(r.get("high")),
            "low":      coerce_float(r.get("low")),
            "close":    coerce_float(r.get("close")),
            "volume":   coerce_int(r.get("volume")),
        })

    store.write_partitioned_parquet("ohlcv.bar", symbol, rows, OHLCV_BAR_SCHEMA)
    return len(rows)


def run() -> None:
    config  = load_json_config(CONFIG)
    symbols: list = config["watchlist"]
    store   = MinioStore(os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis"))

    end   = date.today().strftime("%Y-%m-%d")
    start = (date.today() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    log.info("Stock OHLCV batch ingest | symbols=%s | %s → %s | bucket=%s",
             symbols, start, end, store.bucket)

    total = 0
    for symbol in symbols:
        try:
            n = _ingest_ohlcv(store, symbol, exchange="HOSE", start=start, end=end)
            log.info("%s: %d OHLCV bars → MinIO", symbol, n)
            total += n
        except Exception:
            log.exception("%s: OHLCV ingest failed", symbol)

    log.info("Done | %d stock OHLCV bars → s3://%s", total, store.bucket)

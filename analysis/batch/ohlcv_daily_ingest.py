# Derives daily OHLCV bars from price snapshots stored in MinIO (market-data).
# Runs once at end of day. Reads all price.snapshot Avro files for the target date,
# groups ticks by symbol, and aggregates into one bar per symbol:
#   open   = price of the first tick (by time)
#   high   = maximum price across all ticks
#   low    = minimum price across all ticks
#   close  = price of the last tick (by time)
#   volume = sum of all tick volumes
# Output partition layout in market-analysis:
#   ohlcv.bar/asset_class={stock|crypto}/symbol={symbol}/year=/month=/day=/part-{ts}.parquet
import logging
import os
import time
from collections import defaultdict
from datetime import date, timedelta

from dotenv import load_dotenv

from model.minio_store import MinioStore
from model.schemas import OHLCV_BAR_SCHEMA
from producers.utils import coerce_float, coerce_int

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

LOOKBACK_DAYS = 1


def _asset_class(symbol: str) -> str:
    """Crypto pairs contain '/' (e.g. BTC/USDT); stock tickers do not."""
    return "crypto" if "/" in symbol else "stock"


def _aggregate(ticks: list[dict]) -> dict:
    """Aggregate a time-sorted list of price ticks into one OHLCV bar."""
    prices  = [coerce_float(r.get("price")) for r in ticks]
    volumes = [coerce_int(r.get("volume")) for r in ticks]
    return {
        "time":     ticks[0]["time"][:10] + "T00:00:00+00:00",
        "symbol":   ticks[0]["symbol"],
        "exchange": ticks[0].get("exchange", ""),
        "open":     prices[0],
        "high":     max(prices),
        "low":      min(prices),
        "close":    prices[-1],
        "volume":   sum(volumes),
    }


def _write_bar(store: MinioStore, bar: dict, year: str, month: str, day: str) -> None:
    asset  = _asset_class(bar["symbol"])
    safe   = bar["symbol"].replace("/", "-")
    ts_ms  = int(time.time() * 1000)
    key    = (f"ohlcv.bar/asset_class={asset}/symbol={safe}"
              f"/year={year}/month={month}/day={day}/part-{ts_ms}.parquet")
    store.write_parquet(key, OHLCV_BAR_SCHEMA, [bar])


def run() -> None:
    raw_store      = MinioStore(os.getenv("MINIO_BUCKET", "market-data"))
    analysis_store = MinioStore(os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis"))

    target        = date.today() - timedelta(days=LOOKBACK_DAYS)
    year, month, day = target.strftime("%Y"), target.strftime("%m"), target.strftime("%d")
    date_str      = target.strftime("%Y-%m-%d")
    date_fragment = f"/year={year}/month={month}/day={day}/"

    log.info("OHLCV daily ingest | date=%s | src=%s → dst=%s",
             date_str, raw_store.bucket, analysis_store.bucket)

    symbol_ticks: dict[str, list[dict]] = defaultdict(list)
    for obj in raw_store.list_objects(prefix="price.snapshot/"):
        if date_fragment not in obj.object_name:
            continue
        part = next((p for p in obj.object_name.split("/") if p.startswith("symbol=")), None)
        if part is None:
            continue
        symbol = part[len("symbol="):]
        symbol_ticks[symbol].extend(raw_store.read_avro(obj.object_name))

    if not symbol_ticks:
        log.warning("No price snapshots found for %s — nothing to ingest", date_str)
        return

    counts: dict[str, int] = defaultdict(int)
    for symbol, ticks in symbol_ticks.items():
        ticks.sort(key=lambda r: r["time"])
        bar = _aggregate(ticks)
        _write_bar(analysis_store, bar, year, month, day)
        asset = _asset_class(symbol)
        counts[asset] += 1
        log.info("[%s] %s: %d ticks → 1 OHLCV bar", asset, symbol, len(ticks))

    log.info("Done | stock=%d  crypto=%d bars written for %s",
             counts["stock"], counts["crypto"], date_str)

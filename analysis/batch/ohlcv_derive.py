# Derives daily OHLCV bars from price snapshots already stored in MinIO (market-data).
# Reads price.snapshot Avro files for the target date, groups ticks by symbol,
# then aggregates:
#   open   = price of the first tick (earliest time)
#   high   = maximum price across all ticks
#   low    = minimum price across all ticks
#   close  = price of the last tick (latest time)
#   volume = sum of all tick volumes
# Output is written to market-analysis as Snappy-compressed Parquet,
# using the same partition layout as the external OHLCV ingest jobs.
import logging
import os
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


def run() -> None:
    raw_store      = MinioStore(os.getenv("MINIO_BUCKET", "market-data"))
    analysis_store = MinioStore(os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis"))

    target        = date.today() - timedelta(days=LOOKBACK_DAYS)
    year, month, day = target.strftime("%Y"), target.strftime("%m"), target.strftime("%d")
    date_str      = target.strftime("%Y-%m-%d")
    date_fragment = f"/year={year}/month={month}/day={day}/"

    log.info("Deriving OHLCV from price snapshots | date=%s | src=%s → dst=%s",
             date_str, raw_store.bucket, analysis_store.bucket)

    # Collect all ticks for the target date, grouped by symbol
    symbol_ticks: dict[str, list[dict]] = defaultdict(list)
    for obj in raw_store.list_objects(prefix="price.snapshot/"):
        if date_fragment not in obj.object_name:
            continue
        # Extract symbol from path: price.snapshot/symbol={sym}/year=.../
        part = next((p for p in obj.object_name.split("/") if p.startswith("symbol=")), None)
        if part is None:
            continue
        symbol = part[len("symbol="):]
        symbol_ticks[symbol].extend(raw_store.read_avro(obj.object_name))

    if not symbol_ticks:
        log.warning("No price snapshots found for %s — nothing to derive", date_str)
        return

    total = 0
    for symbol, ticks in symbol_ticks.items():
        ticks.sort(key=lambda r: r["time"])
        bar = _aggregate(ticks)
        analysis_store.write_partitioned_parquet("ohlcv.bar", symbol, [bar], OHLCV_BAR_SCHEMA)
        log.info("%s: derived OHLCV from %d ticks → MinIO", symbol, len(ticks))
        total += 1

    log.info("Done | derived %d OHLCV bars for %s", total, date_str)

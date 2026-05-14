# Derives daily OHLCV bars from price snapshots stored in MinIO (market-data).
# Runs once at end of day. Reads price.snapshot Avro files for the target date
# directly from MinIO via Spark's S3A connector, aggregates into one bar per symbol:
#   open   = price of the first tick (by time)
#   high   = maximum price across all ticks
#   low    = minimum price across all ticks
#   close  = price of the last tick (by time)
#   volume = sum of all tick volumes
# All bars for the same asset class are written into a single Parquet file:
#   ohlcv.bar/asset_class={stock|crypto}/year=/month=/day=/part-{ts}.parquet
import logging
import os
import time
from collections import defaultdict
from datetime import date, timedelta

from dotenv import load_dotenv
from pyspark.sql import functions as F

from model.minio_store import MinioStore
from model.schemas import OHLCV_BAR_SCHEMA
from model.spark import SparkFactory

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

LOOKBACK_DAYS = 0

_COL_ORDER = ["time", "symbol", "exchange", "open", "high", "low", "close", "volume"]


def run() -> None:
    raw_store      = MinioStore(os.getenv("MINIO_BUCKET", "market-data"))
    analysis_store = MinioStore(os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis"))

    target        = date.today() - timedelta(days=LOOKBACK_DAYS)
    year, month, day = target.strftime("%Y"), target.strftime("%m"), target.strftime("%d")
    date_str      = target.strftime("%Y-%m-%d")
    date_fragment = f"/year={year}/month={month}/day={day}/"

    log.info("OHLCV daily ingest | date=%s | src=%s → dst=%s",
             date_str, raw_store.bucket, analysis_store.bucket)

    # Enumerate today's Avro files via MinIO SDK — concrete paths avoid S3A glob issues.
    # (recursiveFileLookup=true disables glob expansion; passing explicit file paths is
    # cleaner and also skips Spark's partition-column inference on the directory hierarchy.)
    today_files = [
        f"s3a://{raw_store.bucket}/{obj.object_name}"
        for obj in raw_store.list_objects(prefix="price.snapshot/")
        if date_fragment in obj.object_name and obj.object_name.endswith(".avro")
    ]
    if not today_files:
        log.warning("No price snapshots found for %s — nothing to ingest", date_str)
        return

    log.info("Found %d Avro files for %s", len(today_files), date_str)

    class_bars: dict[str, list[dict]] = defaultdict(list)
    with SparkFactory("ohlcv_daily_ingest") as spark:
        df = spark.read.format("avro").load(today_files)

        # open/close via struct sort (ISO 8601 strings sort lexicographically = chronologically).
        # min(struct("time","price")) picks the earliest tick; max picks the latest.
        df_ohlcv = (
            df.groupBy("symbol", "exchange")
            .agg(
                F.min(F.struct("time", "price")).getField("price").alias("open"),
                F.max("price").alias("high"),
                F.min("price").alias("low"),
                F.max(F.struct("time", "price")).getField("price").alias("close"),
                F.sum("volume").alias("volume"),
                F.min("time").alias("_min_time"),
            )
            .withColumn("time", F.concat(F.col("_min_time").substr(1, 10), F.lit("T00:00:00+00:00")))
            .withColumn(
                "asset_class",
                F.when(F.col("symbol").contains("/"), F.lit("crypto")).otherwise(F.lit("stock")),
            )
            .drop("_min_time")
        )

        # Collect once; split by asset class in Python before writing to MinIO
        for row in df_ohlcv.collect():
            class_bars[row["asset_class"]].append({c: row[c] for c in _COL_ORDER})

    ts_ms = int(time.time() * 1000)
    for asset, bars in class_bars.items():
        key = (f"ohlcv.bar/asset_class={asset}"
               f"/year={year}/month={month}/day={day}/part-{ts_ms}.parquet")
        analysis_store.write_parquet(key, OHLCV_BAR_SCHEMA, bars)
        symbols = [b["symbol"] for b in bars]
        log.info("[%s] wrote %d bars %s → s3://%s/%s",
                 asset, len(bars), symbols, analysis_store.bucket, key)

    log.info("Done | stock=%d  crypto=%d bars written for %s",
             len(class_bars.get("stock", [])), len(class_bars.get("crypto", [])), date_str)

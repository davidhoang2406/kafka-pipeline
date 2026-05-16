# Derives daily OHLCV bars from price snapshots stored in MinIO (market-data).
# Runs once at end of day. Reads price.snapshot Avro files for the target date
# via Spark glob expansion, aggregates into one bar per symbol:
#   open   = price of the first tick (by time)
#   high   = maximum price across all ticks
#   low    = minimum price across all ticks
#   close  = price of the last tick (by time)
#   volume = sum of all tick volumes
# Spark writes directly to MinIO with mode=overwrite, making re-runs idempotent:
#   s3a://market-analysis/ohlcv.bar/asset_class={stock|crypto}/year=/month=/day=/
#
# Default behaviour is controlled by config/ohlcv_ingest.json.
# Pass --date YYYY-MM-DD via the CLI (main.py ohlcv-daily-ingest --date ...) for backfill.
import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import functions as F

from model.minio_store import MinioStore
from model.spark import SparkFactory

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RAW_BUCKET      = os.getenv("MINIO_BUCKET", "market-data")
ANALYSIS_BUCKET = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")
CONFIG          = Path(__file__).parent.parent.parent / "config" / "ohlcv_ingest.json"


def run(target_date: str | None = None) -> None:
    # CLI --date takes priority; otherwise use lookback_days from config
    if target_date:
        target = date.fromisoformat(target_date)
    else:
        config   = json.loads(CONFIG.read_text())
        lookback = config.get("lookback_days", 0)
        target   = date.today() - timedelta(days=lookback)

    year  = target.strftime("%Y")
    month = target.strftime("%m")
    day   = target.strftime("%d")

    log.info("OHLCV daily ingest | date=%s | src=%s → dst=%s",
             target.isoformat(), RAW_BUCKET, ANALYSIS_BUCKET)

    # Lightweight existence check via MinIO SDK — only to skip startup when no data exists.
    # The file paths returned here are NOT passed to Spark (that would violate the reading rule).
    raw_store     = MinioStore(RAW_BUCKET)
    date_fragment = f"/year={year}/month={month}/day={day}/"
    has_data = any(
        date_fragment in obj.object_name
        for obj in raw_store.list_objects(prefix="price.snapshot/")
    )
    if not has_data:
        log.warning("No price snapshots found for %s — nothing to ingest", target.isoformat())
        return

    # Glob path — Spark expands wildcards natively, no SDK-listed paths involved.
    # A date-scoped glob (not the root prefix) is used because `symbol` appears in both
    # the Avro schema and the partition path, which causes a duplicate-column error when
    # Spark infers all partition levels from the root.
    src_glob = (
        f"s3a://{RAW_BUCKET}/price.snapshot/"
        f"asset_class=*/symbol=*/year={year}/month={month}/day={day}"
    )
    dst = f"s3a://{ANALYSIS_BUCKET}/ohlcv.bar"

    with SparkFactory("ohlcv_daily_ingest") as spark:
        # Dynamic mode overwrites only the target date's partitions, not the whole table
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        df = spark.read.format("avro").load(src_glob)

        # open/close via struct sort (ISO 8601 strings sort lexicographically = chronologically)
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
            # asset_class is in the partition path but not the Avro schema — derive from symbol
            .withColumn("asset_class",
                F.when(F.col("symbol").contains("/"), F.lit("crypto")).otherwise(F.lit("stock")))
            # Partition columns must exist as DataFrame columns before partitionBy
            .withColumn("year",  F.lit(year))
            .withColumn("month", F.lit(month))
            .withColumn("day",   F.lit(day))
            .drop("_min_time")
        )

        # Spark writes all bars directly — no driver collect, no PyArrow serialisation
        (df_ohlcv
         .write
         .mode("overwrite")
         .partitionBy("asset_class", "year", "month", "day")
         .parquet(dst))

        log.info("Done | bars written for %s → %s", target.isoformat(), dst)

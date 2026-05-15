# Reads all ohlcv.bar Parquet from market-analysis, computes SMA20/50/200, RSI14,
# MACD(12/26/9), and Bollinger Bands(20) per symbol using Spark window functions,
# then writes a text report to reports/technical_YYYY-MM-DD.txt.
# Phase 9 — teaches: Window.partitionBy/orderBy/rowsBetween, avg/stddev_pop over a
# rolling window, lag for per-row delta, applyInPandas for EMA-based MACD.
import logging
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

from model.spark import SparkFactory

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

REPORTS_DIR     = Path("reports")
ANALYSIS_BUCKET = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")

# Minimum cumulative bars before an indicator is meaningful
_MIN = {"sma20": 20, "sma50": 50, "sma200": 200, "rsi": 14, "bb": 20, "macd": 26}

_MACD_SCHEMA = StructType([
    StructField("symbol",      StringType()),
    StructField("time",        StringType()),
    StructField("macd",        DoubleType()),
    StructField("macd_signal", DoubleType()),
    StructField("macd_hist",   DoubleType()),
])


def _macd_per_symbol(pdf):
    """applyInPandas function: computes MACD per symbol using pandas EWM (EMA)."""
    import pandas as pd
    pdf = pdf.sort_values("time").copy()
    close  = pdf["close"].astype(float)
    ema12  = close.ewm(span=12, adjust=False).mean()
    ema26  = close.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return pd.DataFrame({
        "symbol":      pdf["symbol"].values,
        "time":        pdf["time"].values,
        "macd":        macd.values,
        "macd_signal": signal.values,
        "macd_hist":   (macd - signal).values,
    })


def _add_indicators(df: DataFrame) -> DataFrame:
    # Window specs defined here — Window.partitionBy() requires an active SparkContext.
    w_ord  = Window.partitionBy("symbol").orderBy("time")
    w20    = w_ord.rowsBetween(-19,  0)
    w50    = w_ord.rowsBetween(-49,  0)
    w200   = w_ord.rowsBetween(-199, 0)
    w14    = w_ord.rowsBetween(-13,  0)

    # Row number within each symbol (1 = oldest bar) — used for minimum-bar guards
    df = df.withColumn("_n", F.row_number().over(w_ord))

    # ── SMA ──────────────────────────────────────────────────────────────────
    df = (df
        .withColumn("sma20",  F.when(F.col("_n") >= _MIN["sma20"],  F.avg("close").over(w20)))
        .withColumn("sma50",  F.when(F.col("_n") >= _MIN["sma50"],  F.avg("close").over(w50)))
        .withColumn("sma200", F.when(F.col("_n") >= _MIN["sma200"], F.avg("close").over(w200)))
    )

    # ── Bollinger Bands (20-day, ±2σ) ────────────────────────────────────────
    bb_mid = F.avg("close").over(w20)
    bb_std = F.stddev_pop("close").over(w20)
    df = (df
        .withColumn("bb_mid",   F.when(F.col("_n") >= _MIN["bb"], bb_mid))
        .withColumn("bb_upper", F.when(F.col("_n") >= _MIN["bb"], bb_mid + 2 * bb_std))
        .withColumn("bb_lower", F.when(F.col("_n") >= _MIN["bb"], bb_mid - 2 * bb_std))
    )

    # ── RSI 14 ───────────────────────────────────────────────────────────────
    # Step 1: per-row gain and loss using lag (avoids illegal nested window functions)
    prev = F.lag("close", 1).over(w_ord)
    df = (df
        .withColumn("_prev", prev)
        .withColumn("_gain", F.when(F.col("close") > F.col("_prev"), F.col("close") - F.col("_prev")).otherwise(F.lit(0.0)))
        .withColumn("_loss", F.when(F.col("close") < F.col("_prev"), F.col("_prev") - F.col("close")).otherwise(F.lit(0.0)))
    )
    # Step 2: 14-period rolling average of gain and loss → RSI
    avg_gain = F.avg("_gain").over(w14)
    avg_loss = F.avg("_loss").over(w14)
    df = df.withColumn(
        "rsi14",
        F.when(F.col("_n") >= _MIN["rsi"],
            F.when(avg_loss == 0, F.lit(100.0))
             .otherwise(100.0 - 100.0 / (1.0 + avg_gain / avg_loss))
        )
    ).drop("_prev", "_gain", "_loss")

    # ── MACD (12/26/9 EMA) via applyInPandas ─────────────────────────────────
    # EMA computation is inherently sequential — cannot be expressed as a Spark
    # window aggregate. applyInPandas ships each symbol's data to a pandas UDF.
    macd_df = (
        df.select("symbol", "time", "close")
          .groupBy("symbol")
          .applyInPandas(_macd_per_symbol, schema=_MACD_SCHEMA)
    )
    df = df.join(macd_df, ["symbol", "time"])
    # Null out MACD for rows with insufficient EWM warmup history
    df = df.withColumn("macd",        F.when(F.col("_n") >= _MIN["macd"], F.col("macd")))
    df = df.withColumn("macd_signal", F.when(F.col("_n") >= _MIN["macd"], F.col("macd_signal")))
    df = df.withColumn("macd_hist",   F.when(F.col("_n") >= _MIN["macd"], F.col("macd_hist")))

    return df.drop("_n")


def _format_row(row) -> str:
    r = row.asDict()
    parts = [f"{r['symbol']:8s}  price={r['close']:10.2f}"]
    for label, key in [("SMA20", "sma20"), ("SMA50", "sma50"), ("SMA200", "sma200")]:
        if r.get(key) is not None:
            parts.append(f"  {label}={r[key]:.2f}")
    if r.get("rsi14") is not None:
        parts.append(f"  RSI14={r['rsi14']:.1f}")
    if r.get("macd") is not None:
        parts.append(
            f"  MACD={r['macd']:.2f}"
            f"  sig={r['macd_signal']:.2f}"
            f"  hist={r['macd_hist']:.2f}"
        )
    if r.get("bb_mid") is not None:
        parts.append(f"  BB[{r['bb_lower']:.2f}|{r['bb_mid']:.2f}|{r['bb_upper']:.2f}]")
    return "".join(parts)


def run() -> None:
    src = f"s3a://{ANALYSIS_BUCKET}/ohlcv.bar"
    log.info("TechnicalJob | source=%s | computing indicators...", src)

    with SparkFactory("TechnicalJob") as spark:
        df = spark.read.parquet(src)
        df = _add_indicators(df)

        # Report only the latest bar per symbol (all window history was used above)
        latest = Window.partitionBy("symbol").orderBy(F.col("time").desc())
        rows = (
            df
            .withColumn("_rn", F.row_number().over(latest))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .orderBy("symbol")
            .collect()
        )

    today    = date.today().isoformat()
    out_path = REPORTS_DIR / f"technical_{today}.txt"
    REPORTS_DIR.mkdir(exist_ok=True)

    header = f"Technical Analysis Report — {today}"
    lines  = [header, "=" * len(header), ""]
    for row in rows:
        line = _format_row(row)
        lines.append(line)
        log.info(line)

    report = "\n".join(lines)
    out_path.write_text(report)
    log.info("Report written to %s (%d symbols)", out_path, len(rows))
    print(report)

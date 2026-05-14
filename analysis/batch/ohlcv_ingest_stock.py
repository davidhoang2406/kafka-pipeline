# Batch ingest for Vietnamese stock OHLCV and financial statements.
# OHLCV bars are written directly to MinIO as Avro (bypasses Kafka).
# Financial statements are still published to the `stock.financials` Kafka topic.
# Intended to run once per day (cron or manual trigger).
import io
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path

import fastavro
from dotenv import load_dotenv
from minio import Minio
from vnstock import Quote

from producers.base_producer import BaseProducer
from producers.utils import coerce_float, coerce_int, load_json_config, to_ts
from schemas.message import build_envelope

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

FINANCIALS_TOPIC = "stock.financials"
CONFIG           = Path(__file__).parent.parent.parent / "config" / "symbols.json"
LOOKBACK_DAYS    = 1

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
    ts_ms   = int(time.time() * 1000)
    symbol  = rows[0]["symbol"]
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


def _ingest_ohlcv(client: Minio, bucket: str, symbol: str, exchange: str, start: str, end: str) -> int:
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

    _write_ohlcv_to_minio(client, bucket, rows)
    return len(rows)


def _publish_financials(producer: BaseProducer, symbol: str, exchange: str) -> int:
    try:
        from vnstock import Finance  # noqa: PLC0415
    except ImportError:
        log.warning("Finance class not available in this vnstock version — skipping financials")
        return 0

    try:
        income  = Finance(symbol=symbol, period="quarter", source="VCI").income_statement(lang="en")
        balance = Finance(symbol=symbol, period="quarter", source="VCI").balance_sheet(lang="en")
    except Exception:
        log.exception("%s: financials fetch failed", symbol)
        return 0

    if income is None or income.empty:
        log.warning("%s: income statement returned empty", symbol)
        return 0

    count = 0
    for i, (_, row) in enumerate(income.iterrows()):
        r = row.to_dict()
        report_date = r.get("year") or r.get("report_date") or r.get("fiscal_date")
        if report_date is None:
            continue

        period_label = str(r.get("quarter", r.get("period", "Q?")))

        bal: dict = {}
        if balance is not None and not balance.empty and i < len(balance):
            bal = balance.iloc[i].to_dict()

        payload = {
            "report_date":  str(report_date)[:10],
            "period":       period_label,
            "revenue":      coerce_float(r.get("revenue") or r.get("net_revenue")),
            "net_income":   coerce_float(r.get("net_income") or r.get("profit_after_tax")),
            "total_assets": coerce_float(bal.get("total_assets")),
            "total_debt":   coerce_float(bal.get("total_debt") or bal.get("short_term_borrowing")),
            "eps":          coerce_float(r.get("eps") or r.get("basic_eps")),
        }
        producer.send(
            FINANCIALS_TOPIC,
            value=build_envelope("financials.report", symbol, exchange, payload),
            key=symbol,
        )
        count += 1

    return count


def run() -> None:
    config  = load_json_config(CONFIG)
    symbols: list = config["watchlist"]
    bucket  = os.getenv("MINIO_BUCKET", "market-data")

    end   = date.today().strftime("%Y-%m-%d")
    start = (date.today() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    log.info("Stock OHLCV batch ingest | symbols=%s | %s → %s", symbols, start, end)

    minio_client = _make_minio_client()

    with BaseProducer() as producer:
        total_ohlcv = 0
        total_fin   = 0

        for symbol in symbols:
            try:
                n = _ingest_ohlcv(minio_client, bucket, symbol, exchange="HOSE", start=start, end=end)
                log.info("%s: %d OHLCV bars → MinIO", symbol, n)
                total_ohlcv += n
            except Exception:
                log.exception("%s: OHLCV ingest failed", symbol)

            try:
                n = _publish_financials(producer, symbol, exchange="HOSE")
                if n:
                    log.info("%s: %d financial reports → %s", symbol, n, FINANCIALS_TOPIC)
                total_fin += n
            except Exception:
                log.exception("%s: financials publish failed", symbol)

        producer.flush()
        log.info(
            "Done | OHLCV: %d bars → MinIO | Financials: %d reports → %s",
            total_ohlcv, FINANCIALS_TOPIC, total_fin, FINANCIALS_TOPIC,
        )

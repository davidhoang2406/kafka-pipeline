# Subscribes to all five Kafka topics (stock + crypto) and persists messages to TimescaleDB.
# Routes by event_type: price.snapshot → price_snapshots, ohlcv.bar → ohlcv_daily,
# financials.report → financials. Batches inserts (up to 100 rows or 10 s) for efficiency.
import logging
import os
import time
from collections import defaultdict

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from consumers.base_consumer import BaseConsumer

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

STOCK_TOPICS  = ["stock.price.realtime", "stock.ohlcv.daily", "stock.financials"]
CRYPTO_TOPICS = ["crypto.price.realtime", "crypto.ohlcv.daily"]
TOPICS        = STOCK_TOPICS + CRYPTO_TOPICS
_CRYPTO_TOPIC_SET = set(CRYPTO_TOPICS)

GROUP_ID       = "storage"
BATCH_SIZE     = 100   # flush after this many rows across all tables
FLUSH_INTERVAL = 10    # also flush after this many seconds even if batch isn't full

# Each entry: (INSERT SQL with %s placeholder, row-extractor lambda)
_ROUTES = {
    "price.snapshot": (
        """INSERT INTO price_snapshots
               (time, symbol, exchange, price, change, pct_change, volume, bid, ask)
           VALUES %s
           ON CONFLICT (time, symbol) DO NOTHING""",
        lambda m: (
            m["timestamp"],
            m["symbol"],
            m["exchange"],
            m["payload"].get("price", 0),
            m["payload"].get("change", 0),
            m["payload"].get("pct_change", 0),
            m["payload"].get("volume", 0),
            m["payload"].get("bid", 0),
            m["payload"].get("ask", 0),
        ),
    ),
    "ohlcv.bar": (
        """INSERT INTO ohlcv_daily
               (time, symbol, exchange, open, high, low, close, volume)
           VALUES %s
           ON CONFLICT (time, symbol) DO NOTHING""",
        lambda m: (
            m["timestamp"],
            m["symbol"],
            m["exchange"],
            m["payload"].get("open", 0),
            m["payload"].get("high", 0),
            m["payload"].get("low", 0),
            m["payload"].get("close", 0),
            m["payload"].get("volume", 0),
        ),
    ),
    "financials.report": (
        """INSERT INTO financials
               (report_date, symbol, period, revenue, net_income, total_assets, total_debt, eps)
           VALUES %s
           ON CONFLICT (report_date, symbol) DO NOTHING""",
        lambda m: (
            m["payload"].get("report_date", m["timestamp"][:10]),
            m["symbol"],
            m["payload"].get("period"),
            m["payload"].get("revenue"),
            m["payload"].get("net_income"),
            m["payload"].get("total_assets"),
            m["payload"].get("total_debt"),
            m["payload"].get("eps"),
        ),
    ),
}


class _Buffer:
    """Accumulates rows per event_type and batch-inserts them into a TimescaleDB database."""

    def __init__(self, conn, name: str):
        self._conn = conn
        self._name = name
        self._rows: dict[str, list] = defaultdict(list)
        self._last_flush = time.monotonic()

    def add(self, msg: dict) -> None:
        event_type = msg.get("event_type")
        if event_type not in _ROUTES:
            log.debug("Skipping unknown event_type=%s", event_type)
            return
        _, extractor = _ROUTES[event_type]
        self._rows[event_type].append(extractor(msg))

    def should_flush(self) -> bool:
        total = sum(len(r) for r in self._rows.values())
        elapsed = time.monotonic() - self._last_flush
        return total >= BATCH_SIZE or elapsed >= FLUSH_INTERVAL

    def flush(self) -> None:
        counts = {k: len(v) for k, v in self._rows.items() if v}
        if not counts:
            self._last_flush = time.monotonic()
            return

        with self._conn.cursor() as cur:
            for event_type, rows in self._rows.items():
                if not rows:
                    continue
                sql, _ = _ROUTES[event_type]
                psycopg2.extras.execute_values(cur, sql, rows)

        self._conn.commit()
        log.info("Flushed → %s | %s", self._name, counts)
        self._rows.clear()
        self._last_flush = time.monotonic()


def run() -> None:
    stock_url  = os.getenv("TIMESCALE_URL",        "postgresql://postgres:password@localhost:5432/stocks")
    crypto_url = os.getenv("TIMESCALE_CRYPTO_URL", "postgresql://postgres:password@localhost:5432/crypto")

    stock_conn  = psycopg2.connect(stock_url)
    crypto_conn = psycopg2.connect(crypto_url)

    log.info("Connected to stocks DB and crypto DB")
    log.info("Subscribing to %s as group '%s'", TOPICS, GROUP_ID)

    stock_buf  = _Buffer(stock_conn,  name="stocks")
    crypto_buf = _Buffer(crypto_conn, name="crypto")

    with BaseConsumer(TOPICS, group_id=GROUP_ID) as consumer:
        while True:
            for msg in consumer.poll(timeout_ms=1000):
                buf = crypto_buf if msg.topic in _CRYPTO_TOPIC_SET else stock_buf
                buf.add(msg.value)

            if stock_buf.should_flush():
                stock_buf.flush()
            if crypto_buf.should_flush():
                crypto_buf.flush()

# Polls live ticker prices from a crypto exchange via CCXT (default: Binance)
# for all trading pairs in config/crypto.json, then publishes price snapshots
# to the `crypto.price.realtime` Kafka topic. Runs as a continuous loop (default: 60 s).
import logging
import time
from pathlib import Path

import ccxt
from dotenv import load_dotenv

from producers.base_producer import BaseProducer
from producers.utils import coerce_float, coerce_int, load_json_config
from schemas.message import build_envelope

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "crypto.price.realtime"
CONFIG = Path(__file__).parent.parent / "config" / "crypto.json"


def _publish_snapshot(
    producer: BaseProducer,
    exchange_client,
    symbols: list,
    exchange_id: str,
) -> int:
    tickers = exchange_client.fetch_tickers(symbols)
    if not tickers:
        log.warning("fetch_tickers returned empty result")
        return 0

    count = 0
    for symbol, ticker in tickers.items():
        payload = {
            "price":      coerce_float(ticker.get("last")),
            "change":     coerce_float(ticker.get("change")),
            "pct_change": coerce_float(ticker.get("percentage")),
            "volume":     coerce_int(ticker.get("quoteVolume")),
            "bid":        coerce_float(ticker.get("bid")),
            "ask":        coerce_float(ticker.get("ask")),
        }
        kafka_key = symbol.replace("/", "-")
        producer.send(
            TOPIC,
            value=build_envelope(
                "price.snapshot",
                symbol,
                exchange_id.upper(),
                payload,
                source=f"ccxt/{exchange_id}",
            ),
            key=kafka_key,
        )
        count += 1

    producer.flush()
    return count


def run() -> None:
    config = load_json_config(CONFIG)
    exchange_id: str = config["exchange"]
    symbols: list = config["symbols"]
    interval: int = config.get("poll_interval_seconds", 30)

    exchange_client = getattr(ccxt, exchange_id)()

    log.info(
        "Starting crypto price producer | exchange=%s | symbols=%s | interval=%ds",
        exchange_id, symbols, interval,
    )

    with BaseProducer() as producer:
        while True:
            try:
                n = _publish_snapshot(producer, exchange_client, symbols, exchange_id)
                log.info("Published %d crypto price snapshots → %s", n, TOPIC)
            except Exception:
                log.exception("Fetch failed — retrying in %ds", interval)
            time.sleep(interval)

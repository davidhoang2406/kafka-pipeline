import logging
from datetime import datetime, timezone
from pathlib import Path

import ccxt
from dotenv import load_dotenv

from producers.base_producer import BaseProducer
from producers.utils import coerce_float, coerce_int, load_json_config
from schemas.message import build_envelope

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "crypto.ohlcv.daily"
CONFIG = Path(__file__).parent.parent / "config" / "crypto.json"


def _publish_ohlcv(
    producer: BaseProducer,
    exchange_client,
    symbol: str,
    exchange_id: str,
    lookback: int,
) -> int:
    bars = exchange_client.fetch_ohlcv(symbol, timeframe="1d", limit=lookback)
    if not bars:
        log.warning("%s: fetch_ohlcv returned empty", symbol)
        return 0

    kafka_key = symbol.replace("/", "-")
    count = 0
    for bar in bars:
        ts_ms, open_, high, low, close, volume = bar
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
        payload = {
            "open":   coerce_float(open_),
            "high":   coerce_float(high),
            "low":    coerce_float(low),
            "close":  coerce_float(close),
            "volume": coerce_int(volume),
        }
        producer.send(
            TOPIC,
            value=build_envelope(
                "ohlcv.bar",
                symbol,
                exchange_id.upper(),
                payload,
                timestamp=ts,
                source=f"ccxt/{exchange_id}",
            ),
            key=kafka_key,
        )
        count += 1

    return count


def run() -> None:
    config = load_json_config(CONFIG)
    exchange_id: str = config["exchange"]
    symbols: list = config["symbols"]
    lookback: int = config.get("ohlcv_lookback_days", 1)

    exchange_client = getattr(ccxt, exchange_id)()

    log.info(
        "Starting crypto OHLCV producer | exchange=%s | symbols=%s | lookback=%d days",
        exchange_id, symbols, lookback,
    )

    with BaseProducer() as producer:
        total = 0
        for symbol in symbols:
            try:
                n = _publish_ohlcv(producer, exchange_client, symbol, exchange_id, lookback)
                log.info("%s: %d OHLCV bars → %s", symbol, n, TOPIC)
                total += n
            except Exception:
                log.exception("%s: OHLCV fetch failed", symbol)

        producer.flush()
        log.info("Done | %d crypto OHLCV bars → %s", total, TOPIC)

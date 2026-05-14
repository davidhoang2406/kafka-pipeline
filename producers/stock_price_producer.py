# Polls the vnstock price board (KBS source) for all symbols in config/stocks.json
# and publishes real-time price snapshots to the `stock.price.realtime` Kafka topic.
# Runs as a continuous loop at a configurable interval (default: 30 s).
import logging
import time
from pathlib import Path

from dotenv import load_dotenv
from vnstock import Trading  # requires Python 3.10+

from producers.base_producer import BaseProducer
from producers.utils import coerce_float, coerce_int, load_json_config
from schemas.message import build_envelope

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "stock.price.realtime"
CONFIG = Path(__file__).parent.parent / "config" / "stocks.json"


def _publish_snapshot(producer: BaseProducer, symbols: list, default_exchange: str) -> int:
    df = Trading(source="KBS").price_board(symbols)
    if df is None or df.empty:
        log.warning("price_board returned empty result")
        return 0

    count = 0
    for _, row in df.iterrows():
        r = row.to_dict()

        symbol   = str(r.get("symbol", "UNKNOWN")).upper()
        exchange = str(r.get("exchange") or default_exchange).upper()
        payload = {
            "price":      coerce_float(r.get("close_price")),
            "change":     coerce_float(r.get("price_change")),
            "pct_change": coerce_float(r.get("percent_change")),
            "volume":     coerce_int(r.get("volume_accumulated")),
            "bid":        coerce_float(r.get("bid_price_1")),
            "ask":        coerce_float(r.get("ask_price_1")),
        }
        producer.send(
            TOPIC,
            value=build_envelope("price.snapshot", symbol, exchange, payload),
            key=symbol,
        )
        count += 1

    producer.flush()
    return count


def run():
    config = load_json_config(CONFIG)
    exchange: str  = config["exchange"]
    symbols: list  = config["symbols"]
    interval: int  = config.get("poll_interval_seconds", 300)

    log.info("Starting price producer | exchange=%s | symbols=%s | interval=%ds", exchange, symbols, interval)
    with BaseProducer() as producer:
        while True:
            try:
                n = _publish_snapshot(producer, symbols, exchange)
                log.info("Published %d price snapshots → %s", n, TOPIC)
            except Exception:
                log.exception("Fetch failed — retrying in %ds", interval)
            time.sleep(interval)

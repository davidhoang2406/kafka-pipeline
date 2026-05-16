# Subscribes to stock.price.realtime and crypto.price.realtime and evaluates each
# price snapshot against threshold rules defined in config/alerts.json.
# Prints an alert to the console whenever a rule is triggered. Lightweight alternative
# to the Flink PriceAlertJob — no JVM required, runs directly with Python.
import logging
from datetime import datetime, timezone
from pathlib import Path

from consumers.base_consumer import BaseConsumer
from producers.utils import evaluate_rules, load_json_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPICS = ["stock.price.realtime", "crypto.price.realtime"]
ALERTS_CONFIG = Path(__file__).parent.parent / "config" / "alerts.json"


def _check(rules: list[dict], symbol: str, payload: dict, source: str = "") -> None:
    triggered = evaluate_rules(rules, symbol, payload, source)
    if not triggered:
        return
    now   = datetime.now(timezone.utc).strftime("%H:%M:%S")
    price = payload.get("price", 0.0)
    pct   = payload.get("pct_change", 0.0)
    for hit in triggered:
        print(
            f"[ALERT {now}] {symbol:10s} | {hit['message']}"
            f" | price={price:.2f}  pct={pct:+.2f}%"
            f"  {hit['matched_field']}={hit['matched_value']}"
        )


def run():
    rules = load_json_config(ALERTS_CONFIG)
    log.info("Alert consumer started | %d rules loaded | topics=%s", len(rules), TOPICS)

    with BaseConsumer(TOPICS, group_id="alerts", auto_offset_reset="latest") as consumer:
        for record in consumer.messages():
            msg     = record.value
            symbol  = msg.get("symbol", "")
            payload = msg.get("payload", {})
            source  = msg.get("source", "")
            _check(rules, symbol, payload, source)

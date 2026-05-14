# Subscribes to stock.price.realtime and crypto.price.realtime and evaluates each
# price snapshot against threshold rules defined in config/alerts.json.
# Prints an alert to the console whenever a rule is triggered. Lightweight alternative
# to the Flink PriceAlertJob — no JVM required, runs directly with Python.
import logging
from datetime import datetime, timezone
from pathlib import Path

from consumers.base_consumer import BaseConsumer
from producers.utils import ALERT_OPS, asset_class, load_json_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPICS = ["stock.price.realtime", "crypto.price.realtime"]
ALERTS_CONFIG = Path(__file__).parent.parent / "config" / "alerts.json"


def _check(rules: list[dict], symbol: str, payload: dict, source: str = "") -> None:
    asset_class_ = asset_class(source)
    now   = datetime.now(timezone.utc).strftime("%H:%M:%S")
    price = payload.get("price", 0.0)
    pct   = payload.get("pct_change", 0.0)

    for rule in rules:
        rule_source = rule.get("source", "*")
        if rule_source != "*" and rule_source != asset_class_:
            continue
        if rule["symbol"] != "*" and rule["symbol"] != symbol:
            continue
        field = rule["field"]
        value = payload.get(field)
        if value is None:
            continue
        op_fn = ALERT_OPS.get(rule["operator"])
        if op_fn and op_fn(value, rule["threshold"]):
            print(
                f"[ALERT {now}] {symbol:10s} | {rule['message']}"
                f" | price={price:.2f}  pct={pct:+.2f}%  {field}={value}"
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

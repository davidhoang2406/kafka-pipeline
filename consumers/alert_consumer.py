import json
import logging
import operator
from datetime import datetime, timezone
from pathlib import Path

from consumers.base_consumer import BaseConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPICS = ["stock.price.realtime", "crypto.price.realtime"]
ALERTS_CONFIG = Path(__file__).parent.parent / "config" / "alerts.json"

_OPS = {
    "<":  operator.lt,
    ">":  operator.gt,
    "<=": operator.le,
    ">=": operator.ge,
    "==": operator.eq,
}


def _load_rules() -> list[dict]:
    with open(ALERTS_CONFIG) as f:
        return json.load(f)


def _asset_class(source: str) -> str:
    """Map the envelope source field to the asset-class token used in rules."""
    if source.startswith("vnstock"):
        return "stock"
    if source.startswith("ccxt"):
        return "crypto"
    return "unknown"


def _check(rules: list[dict], symbol: str, payload: dict, source: str = "") -> None:
    asset_class = _asset_class(source)
    now   = datetime.now(timezone.utc).strftime("%H:%M:%S")
    price = payload.get("price", 0.0)
    pct   = payload.get("pct_change", 0.0)

    for rule in rules:
        rule_source = rule.get("source", "*")
        if rule_source != "*" and rule_source != asset_class:
            continue
        if rule["symbol"] != "*" and rule["symbol"] != symbol:
            continue
        field = rule["field"]
        value = payload.get(field)
        if value is None:
            continue
        op_fn = _OPS.get(rule["operator"])
        if op_fn and op_fn(value, rule["threshold"]):
            print(
                f"[ALERT {now}] {symbol:10s} | {rule['message']}"
                f" | price={price:.2f}  pct={pct:+.2f}%  {field}={value}"
            )


def run():
    rules = _load_rules()
    log.info("Alert consumer started | %d rules loaded | topics=%s", len(rules), TOPICS)

    with BaseConsumer(TOPICS, group_id="alerts", auto_offset_reset="latest") as consumer:
        for record in consumer.messages():
            msg     = record.value
            symbol  = msg.get("symbol", "")
            payload = msg.get("payload", {})
            source  = msg.get("source", "")
            _check(rules, symbol, payload, source)

import json
import logging
import operator
from pathlib import Path

log = logging.getLogger(__name__)

# ── Alert helpers ──────────────────────────────────────────────────────────────

ALERT_OPS: dict = {
    "<":  operator.lt,
    ">":  operator.gt,
    "<=": operator.le,
    ">=": operator.ge,
    "==": operator.eq,
}


def asset_class(source: str) -> str:
    """Map an envelope source string to its asset-class token (stock / crypto)."""
    if source.startswith("vnstock"):
        return "stock"
    if source.startswith("ccxt"):
        return "crypto"
    return "unknown"


def coerce_float(v, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def coerce_int(v, default: int = 0) -> int:
    try:
        return int(float(v)) if v is not None else default
    except (TypeError, ValueError):
        return default


def to_ts(v) -> str:
    """Convert a date-like value to an ISO-8601 UTC timestamp string."""
    if hasattr(v, "isoformat"):
        s = v.isoformat()
    else:
        s = str(v)
    # Bare date "YYYY-MM-DD" → add UTC midnight to produce a full ISO-8601 timestamp
    if len(s) == 10:
        s += "T00:00:00+00:00"
    return s


def load_json_config(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def validate_rules(rules: list[dict]) -> list[dict]:
    """Drop rules with unknown operators at load time so evaluate_rules never silently no-ops."""
    valid = []
    for rule in rules:
        op = rule.get("operator")
        if op not in ALERT_OPS:
            log.warning("Skipping alert rule with unknown operator %r: %s", op, rule)
            continue
        valid.append(rule)
    return valid


def evaluate_rules(rules: list[dict], symbol: str, payload: dict, source: str = "") -> list[dict]:
    """Return every rule that fires for this tick.

    Each returned dict is the original rule entry plus two extra keys:
      matched_field  — the payload field that was tested
      matched_value  — the value that crossed the threshold
    """
    asset_class_ = asset_class(source)
    triggered = []
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
            triggered.append({**rule, "matched_field": field, "matched_value": value})
    return triggered

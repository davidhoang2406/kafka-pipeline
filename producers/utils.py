import json
from pathlib import Path


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

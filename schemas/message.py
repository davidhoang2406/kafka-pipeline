from datetime import datetime, timezone


def build_envelope(
    event_type: str,
    symbol: str,
    exchange: str,
    payload: dict,
    timestamp: str | None = None,
    source: str = "vnstock/KBS",
) -> dict:
    return {
        "event_type": event_type,
        "symbol": symbol,
        "exchange": exchange,
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "source": source,
        "payload": payload,
    }

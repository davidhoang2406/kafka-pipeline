import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from consumers.storage_consumer import _SCHEMAS, _EXTRACTORS, _Buffer
from model.minio_store import MinioStore


def _mock_store() -> MinioStore:
    store = MinioStore.__new__(MinioStore)
    store.bucket  = "test-bucket"
    store._client = MagicMock()
    return store

TS = "2024-05-10T08:00:00+00:00"


def _msg(event_type, symbol="VCB", exchange="HOSE", timestamp=TS, **payload):
    return {
        "event_type": event_type,
        "symbol":     symbol,
        "exchange":   exchange,
        "timestamp":  timestamp,
        "payload":    payload,
    }


@pytest.mark.unit
def test_price_snapshot_fields():
    row = _EXTRACTORS["price.snapshot"](_msg(
        "price.snapshot",
        price=85000.0, change=500.0, pct_change=0.59,
        volume=1_000_000, bid=84900.0, ask=85100.0,
    ))
    assert row["time"]       == datetime.fromisoformat(TS).astimezone(timezone.utc)
    assert row["symbol"]     == "VCB"
    assert row["exchange"]   == "HOSE"
    assert row["price"]      == 85000.0
    assert row["change"]     == 500.0
    assert row["pct_change"] == 0.59
    assert row["volume"]     == 1_000_000
    assert row["bid"]        == 84900.0
    assert row["ask"]        == 85100.0


@pytest.mark.unit
def test_unknown_event_type_is_ignored():
    buf = _Buffer(_mock_store())
    buf.add(_msg("market.rumour"))
    assert sum(len(v) for v in buf._rows.values()) == 0


@pytest.mark.unit
def test_buffer_accumulates_multiple_rows():
    buf = _Buffer(_mock_store())
    for i in range(5):
        buf.add(_msg("price.snapshot", price=float(i)))
    assert buf.total_rows() == 5


@pytest.mark.unit
def test_should_flush_after_batch_size(monkeypatch):
    import consumers.storage_consumer as sc
    monkeypatch.setattr(sc, "BATCH_SIZE", 3)
    buf = _Buffer(_mock_store())
    for i in range(3):
        buf.add(_msg("price.snapshot", price=float(i)))
    assert buf.should_flush() is True


@pytest.mark.unit
def test_should_not_flush_before_batch_or_interval():
    buf = _Buffer(_mock_store())
    buf.add(_msg("price.snapshot", price=1.0))
    assert buf.should_flush() is False


# ── DLQ ───────────────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_malformed_message_goes_to_dlq(monkeypatch):
    """A message whose extractor raises must be written to dead-letter/, not buffered."""
    store = _mock_store()
    buf   = _Buffer(store)

    # Force the extractor to raise
    monkeypatch.setitem(
        __import__("consumers.storage_consumer", fromlist=["_EXTRACTORS"])._EXTRACTORS,
        "price.snapshot",
        lambda m: (_ for _ in ()).throw(ValueError("bad field")),
    )

    buf.add(_msg("price.snapshot", price=1.0))

    assert buf.total_rows() == 0
    store._client.put_object.assert_called_once()
    key_arg = store._client.put_object.call_args[0][1]
    assert key_arg.startswith("dead-letter/price.snapshot/")


@pytest.mark.unit
def test_dlq_write_failure_does_not_propagate(monkeypatch):
    """If the DLQ write itself fails, the consumer must not crash."""
    store = _mock_store()
    store._client.put_object.side_effect = Exception("MinIO down")
    buf   = _Buffer(store)

    monkeypatch.setitem(
        __import__("consumers.storage_consumer", fromlist=["_EXTRACTORS"])._EXTRACTORS,
        "price.snapshot",
        lambda m: (_ for _ in ()).throw(ValueError("bad")),
    )

    buf.add(_msg("price.snapshot", price=1.0))  # must not raise
    assert buf.total_rows() == 0

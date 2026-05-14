import pytest
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
    assert row["time"]       == TS
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
    assert len(buf._rows["price.snapshot"]) == 5


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

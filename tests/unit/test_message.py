import pytest

from schemas.message import build_envelope


@pytest.mark.unit
def test_all_fields_present():
    msg = build_envelope("price.snapshot", "VCB", "HOSE", {"price": 85000})
    assert set(msg) == {"event_type", "symbol", "exchange", "timestamp", "source", "payload"}


@pytest.mark.unit
def test_field_values():
    msg = build_envelope("price.snapshot", "VCB", "HOSE", {"price": 85000})
    assert msg["event_type"] == "price.snapshot"
    assert msg["symbol"]     == "VCB"
    assert msg["exchange"]   == "HOSE"
    assert msg["source"]     == "vnstock/KBS"
    assert msg["payload"]    == {"price": 85000}


@pytest.mark.unit
def test_default_timestamp_is_utc():
    msg = build_envelope("price.snapshot", "VCB", "HOSE", {})
    assert msg["timestamp"].endswith("+00:00")


@pytest.mark.unit
def test_custom_timestamp_passes_through():
    ts = "2024-05-10T00:00:00+00:00"
    msg = build_envelope("ohlcv.bar", "VCB", "HOSE", {}, timestamp=ts)
    assert msg["timestamp"] == ts


@pytest.mark.unit
def test_default_used_when_timestamp_is_none():
    msg = build_envelope("ohlcv.bar", "VCB", "HOSE", {}, timestamp=None)
    assert msg["timestamp"].endswith("+00:00")


@pytest.mark.unit
def test_payload_is_not_mutated():
    original = {"price": 85000}
    build_envelope("price.snapshot", "VCB", "HOSE", original)
    assert original == {"price": 85000}

import pytest
from unittest.mock import MagicMock

from consumers.storage_consumer import _ROUTES, _Buffer

# ── helpers ───────────────────────────────────────────────────────────────────

TS = "2024-05-10T08:00:00+00:00"


def _msg(event_type, symbol="VCB", exchange="HOSE", timestamp=TS, **payload):
    return {
        "event_type": event_type,
        "symbol":     symbol,
        "exchange":   exchange,
        "timestamp":  timestamp,
        "payload":    payload,
    }


# ── route extractor column mapping ────────────────────────────────────────────
# INSERT order: (time[0], symbol[1], exchange[2], price[3], change[4],
#                pct_change[5], volume[6], bid[7], ask[8])

@pytest.mark.unit
def test_price_snapshot_column_order():
    _, ext = _ROUTES["price.snapshot"]
    row = ext(_msg("price.snapshot",
                   price=85000.0, change=500.0, pct_change=0.59,
                   volume=1_000_000, bid=84900.0, ask=85100.0))
    assert row[0] == TS
    assert row[1] == "VCB"
    assert row[2] == "HOSE"
    assert row[3] == 85000.0
    assert row[4] == 500.0
    assert row[5] == 0.59
    assert row[6] == 1_000_000
    assert row[7] == 84900.0
    assert row[8] == 85100.0


# INSERT order: (time[0], symbol[1], exchange[2], open[3], high[4],
#                low[5], close[6], volume[7])

@pytest.mark.unit
def test_ohlcv_bar_column_order():
    _, ext = _ROUTES["ohlcv.bar"]
    row = ext(_msg("ohlcv.bar",
                   open=84000.0, high=86000.0, low=83500.0,
                   close=85500.0, volume=2_000_000))
    assert row[0] == TS
    assert row[1] == "VCB"
    assert row[2] == "HOSE"
    assert row[3] == 84000.0   # open
    assert row[4] == 86000.0   # high
    assert row[5] == 83500.0   # low
    assert row[6] == 85500.0   # close
    assert row[7] == 2_000_000


# INSERT order: (report_date[0], symbol[1], period[2], revenue[3],
#                net_income[4], total_assets[5], total_debt[6], eps[7])

@pytest.mark.unit
def test_financials_report_column_order():
    _, ext = _ROUTES["financials.report"]
    row = ext(_msg("financials.report",
                   report_date="2024-03-31", period="Q1",
                   revenue=1_000_000.0, net_income=200_000.0,
                   total_assets=5_000_000.0, total_debt=1_500_000.0,
                   eps=2.5))
    assert row[0] == "2024-03-31"
    assert row[1] == "VCB"
    assert row[2] == "Q1"
    assert row[3] == 1_000_000.0
    assert row[4] == 200_000.0
    assert row[5] == 5_000_000.0
    assert row[6] == 1_500_000.0
    assert row[7] == 2.5


@pytest.mark.unit
def test_financials_falls_back_to_envelope_timestamp():
    _, ext = _ROUTES["financials.report"]
    row = ext(_msg("financials.report", period="Q1"))  # no report_date in payload
    assert row[0] == TS[:10]   # "2024-05-10"


# ── _Buffer behaviour ─────────────────────────────────────────────────────────

@pytest.mark.unit
def test_unknown_event_type_is_ignored():
    buf = _Buffer(MagicMock(), name="test")
    buf.add(_msg("market.rumour"))
    assert sum(len(v) for v in buf._rows.values()) == 0


@pytest.mark.unit
def test_buffer_accumulates_multiple_rows():
    buf = _Buffer(MagicMock(), name="test")
    for i in range(5):
        buf.add(_msg("price.snapshot", price=float(i)))
    assert len(buf._rows["price.snapshot"]) == 5


@pytest.mark.unit
def test_should_flush_after_batch_size(monkeypatch):
    import consumers.storage_consumer as sc
    monkeypatch.setattr(sc, "BATCH_SIZE", 3)
    buf = _Buffer(MagicMock(), name="test")
    for i in range(3):
        buf.add(_msg("price.snapshot", price=float(i)))
    assert buf.should_flush() is True


@pytest.mark.unit
def test_should_not_flush_before_batch_or_interval():
    buf = _Buffer(MagicMock(), name="test")
    buf.add(_msg("price.snapshot", price=1.0))
    assert buf.should_flush() is False

import psycopg2.extras
import pytest

from consumers.storage_consumer import _ROUTES
from schemas.message import build_envelope
from tests.conftest import TEST_SYMBOL

TRADING_DATE = "2000-06-15T00:00:00+00:00"


@pytest.mark.integration
def test_ohlcv_bar_uses_trading_date_not_insertion_time(db_conn):
    """
    The envelope timestamp for an OHLCV bar is the actual trading date.
    Verify that TimescaleDB stores that date as `time`, not the insertion time.
    """
    msg = build_envelope(
        "ohlcv.bar", TEST_SYMBOL, "HOSE",
        {"open": 84000.0, "high": 86000.0, "low": 83500.0,
         "close": 85500.0, "volume": 2_000_000},
        timestamp=TRADING_DATE,
    )

    sql, extractor = _ROUTES["ohlcv.bar"]
    with db_conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, [extractor(msg)])
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT time, close, volume FROM ohlcv_daily WHERE symbol = %s AND time = %s",
            (TEST_SYMBOL, TRADING_DATE),
        )
        row = cur.fetchone()

    assert row is not None, "OHLCV bar not found in ohlcv_daily"
    stored_time = row[0].isoformat()
    assert "2000-06-15" in stored_time, (
        f"Expected trading date 2000-06-15 in time column, got {stored_time}"
    )
    assert float(row[1]) == 85500.0   # close
    assert row[2] == 2_000_000        # volume


@pytest.mark.integration
def test_ohlcv_duplicate_idempotent(db_conn):
    msg = build_envelope(
        "ohlcv.bar", TEST_SYMBOL, "HOSE",
        {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        timestamp=TRADING_DATE,
    )

    sql, extractor = _ROUTES["ohlcv.bar"]
    row = extractor(msg)
    with db_conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, [row])
        psycopg2.extras.execute_values(cur, sql, [row])  # duplicate
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ohlcv_daily WHERE symbol = %s AND time = %s",
            (TEST_SYMBOL, TRADING_DATE),
        )
        assert cur.fetchone()[0] == 1

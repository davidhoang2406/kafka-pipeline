import io

import pyarrow.parquet as pq
import pytest

from consumers.storage_consumer import _Buffer, _EXTRACTORS
from schemas.message import build_envelope
from tests.conftest import MINIO_BUCKET, TEST_SYMBOL

TRADING_DATE = "2000-06-15"
TRADING_TS   = f"{TRADING_DATE}T00:00:00+00:00"


def _ohlcv_msg(**kwargs) -> dict:
    payload = {"open": 84000.0, "high": 86000.0, "low": 83500.0,
               "close": 85500.0, "volume": 2_000_000, **kwargs}
    return build_envelope("ohlcv.bar", TEST_SYMBOL, "HOSE", payload, timestamp=TRADING_TS)


def _read_parquet(minio_client, prefix: str):
    objects = list(minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
    assert objects, f"No Parquet file found under s3://{MINIO_BUCKET}/{prefix}"
    response = minio_client.get_object(MINIO_BUCKET, objects[0].object_name)
    try:
        return pq.read_table(io.BytesIO(response.read())).to_pandas()
    finally:
        response.close()
        response.release_conn()


@pytest.mark.integration
def test_ohlcv_bar_uses_trading_date_not_insertion_time(minio_client):
    """The envelope timestamp (trading date) must be stored in the `time` field."""
    msg = _ohlcv_msg()
    buf = _Buffer(minio_client, MINIO_BUCKET)
    buf.add(msg)
    buf.flush()

    prefix = f"ohlcv.bar/symbol={TEST_SYMBOL}/date={TRADING_DATE}/"
    df = _read_parquet(minio_client, prefix)

    row = df[df["symbol"] == TEST_SYMBOL]
    assert len(row) >= 1
    assert TRADING_DATE in row.iloc[0]["time"], (
        f"Expected trading date {TRADING_DATE} in time field, got {row.iloc[0]['time']}"
    )
    assert float(row.iloc[0]["close"]) == 85500.0
    assert int(row.iloc[0]["volume"]) == 2_000_000


@pytest.mark.integration
def test_ohlcv_parquet_schema(minio_client):
    """Written Parquet file must contain all expected OHLCV columns."""
    msg = _ohlcv_msg()
    buf = _Buffer(minio_client, MINIO_BUCKET)
    buf.add(msg)
    buf.flush()

    prefix = f"ohlcv.bar/symbol={TEST_SYMBOL}/date={TRADING_DATE}/"
    df = _read_parquet(minio_client, prefix)

    for col in ("time", "symbol", "exchange", "open", "high", "low", "close", "volume"):
        assert col in df.columns, f"Missing column: {col}"


@pytest.mark.integration
def test_ohlcv_extractor_fields():
    """_EXTRACTORS maps OHLCV envelope payload to the correct row fields."""
    msg = _ohlcv_msg(close=99.0, volume=42)
    row = _EXTRACTORS["ohlcv.bar"](msg)
    assert row["close"] == 99.0
    assert row["volume"] == 42
    assert row["time"] == TRADING_TS
    assert row["symbol"] == TEST_SYMBOL

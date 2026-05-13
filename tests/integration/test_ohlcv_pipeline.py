import io

import fastavro
import pytest

from consumers.storage_consumer import _Buffer, _EXTRACTORS
from schemas.message import build_envelope
from tests.conftest import MINIO_BUCKET, TEST_SYMBOL

TRADING_DATE   = "2000-06-15"
TRADING_TS     = f"{TRADING_DATE}T00:00:00+00:00"
TRADING_PREFIX = f"ohlcv.bar/symbol={TEST_SYMBOL}/year=2000/month=06/day=15/"


def _ohlcv_msg(**kwargs) -> dict:
    payload = {"open": 84000.0, "high": 86000.0, "low": 83500.0,
               "close": 85500.0, "volume": 2_000_000, **kwargs}
    return build_envelope("ohlcv.bar", TEST_SYMBOL, "HOSE", payload, timestamp=TRADING_TS)


def _read_avro(minio_client, prefix: str) -> list[dict]:
    objects = list(minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
    assert objects, f"No Avro file found under s3://{MINIO_BUCKET}/{prefix}"
    response = minio_client.get_object(MINIO_BUCKET, objects[0].object_name)
    try:
        return list(fastavro.reader(io.BytesIO(response.read())))
    finally:
        response.close()
        response.release_conn()


@pytest.mark.integration
def test_ohlcv_bar_uses_trading_date_not_insertion_time(minio_client):
    """The envelope timestamp (trading date) must drive the partition path."""
    buf = _Buffer(minio_client, MINIO_BUCKET)
    buf.add(_ohlcv_msg())
    buf.flush()

    records = _read_avro(minio_client, TRADING_PREFIX)
    row = next(r for r in records if r["symbol"] == TEST_SYMBOL)
    assert TRADING_DATE in row["time"], (
        f"Expected trading date {TRADING_DATE} in time field, got {row['time']}"
    )
    assert float(row["close"]) == 85500.0
    assert int(row["volume"]) == 2_000_000


@pytest.mark.integration
def test_ohlcv_avro_schema(minio_client):
    """Written Avro file must contain all expected OHLCV fields."""
    buf = _Buffer(minio_client, MINIO_BUCKET)
    buf.add(_ohlcv_msg())
    buf.flush()

    records = _read_avro(minio_client, TRADING_PREFIX)
    assert records
    for field in ("time", "symbol", "exchange", "open", "high", "low", "close", "volume"):
        assert field in records[0], f"Missing field: {field}"


@pytest.mark.integration
def test_ohlcv_extractor_fields():
    """_EXTRACTORS maps OHLCV envelope payload to the correct row fields."""
    msg = _ohlcv_msg(close=99.0, volume=42)
    row = _EXTRACTORS["ohlcv.bar"](msg)
    assert row["close"] == 99.0
    assert row["volume"] == 42
    assert row["time"] == TRADING_TS
    assert row["symbol"] == TEST_SYMBOL

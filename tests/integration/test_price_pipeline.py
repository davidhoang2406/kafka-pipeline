import io
import json
import uuid

import pyarrow.parquet as pq
import pytest
from kafka import KafkaConsumer

from consumers.storage_consumer import _Buffer, _EXTRACTORS
from schemas.message import build_envelope
from tests.conftest import MINIO_BUCKET, TEST_SYMBOL

TOPIC      = "stock.price.realtime"
FIXED_DATE = "2000-01-01"
FIXED_TS   = f"{FIXED_DATE}T08:00:00+00:00"


def _price_msg(price: float = 12345.0) -> dict:
    return build_envelope(
        "price.snapshot", TEST_SYMBOL, "HOSE",
        {"price": price, "change": 100.0, "pct_change": 0.81,
         "volume": 500_000, "bid": 12300.0, "ask": 12400.0},
        timestamp=FIXED_TS,
    )


def _read_parquet(minio_client, prefix: str):
    """Download the first Parquet file under prefix and return a pandas DataFrame."""
    objects = list(minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
    assert objects, f"No Parquet file found under s3://{MINIO_BUCKET}/{prefix}"
    response = minio_client.get_object(MINIO_BUCKET, objects[0].object_name)
    try:
        return pq.read_table(io.BytesIO(response.read())).to_pandas()
    finally:
        response.close()
        response.release_conn()


# ── tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.integration
def test_price_snapshot_written_to_minio(minio_client):
    """_Buffer should write a Parquet file and the row should be readable back."""
    msg = _price_msg(price=12345.0)
    buf = _Buffer(minio_client, MINIO_BUCKET)
    buf.add(msg)
    buf.flush()

    prefix = f"price.snapshot/symbol={TEST_SYMBOL}/date={FIXED_DATE}/"
    df = _read_parquet(minio_client, prefix)

    row = df[df["symbol"] == TEST_SYMBOL]
    assert len(row) == 1
    assert float(row.iloc[0]["price"]) == 12345.0
    assert row.iloc[0]["exchange"] == "HOSE"


@pytest.mark.integration
def test_parquet_partition_path_structure(minio_client):
    """Parquet files must be stored under the correct partition prefix."""
    msg = _price_msg()
    buf = _Buffer(minio_client, MINIO_BUCKET)
    buf.add(msg)
    buf.flush()

    expected_prefix = f"price.snapshot/symbol={TEST_SYMBOL}/date={FIXED_DATE}/"
    objects = list(minio_client.list_objects(MINIO_BUCKET, prefix=expected_prefix, recursive=True))
    assert objects, f"Expected objects under {expected_prefix}"


@pytest.mark.integration
def test_extractor_produces_correct_fields():
    """_EXTRACTORS must map all envelope fields to the Parquet row schema."""
    msg = _price_msg(price=99999.0)
    row = _EXTRACTORS["price.snapshot"](msg)
    assert row["price"] == 99999.0
    assert row["symbol"] == TEST_SYMBOL
    assert row["exchange"] == "HOSE"
    assert row["time"] == FIXED_TS


@pytest.mark.integration
def test_consumer_group_isolation(kafka_producer, kafka_bootstrap, unique_group):
    """Two consumers in different groups each receive the same message."""
    test_id = str(uuid.uuid4())
    msg = build_envelope(
        "price.snapshot", "VCB", "HOSE",
        {"price": 85000.0, "test_id": test_id},
    )
    kafka_producer.send(TOPIC, value=msg, key="VCB")
    kafka_producer.flush()

    def consume_one(group_id: str):
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=kafka_bootstrap,
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda b: json.loads(b.decode()),
            consumer_timeout_ms=8000,
        )
        result = None
        for record in consumer:
            if record.value.get("payload", {}).get("test_id") == test_id:
                result = record.value
                break
        consumer.close()
        return result

    group_a = f"test-a-{uuid.uuid4().hex[:8]}"
    group_b = f"test-b-{uuid.uuid4().hex[:8]}"
    assert consume_one(group_a) is not None, "Group A did not receive the message"
    assert consume_one(group_b) is not None, "Group B did not receive the message"

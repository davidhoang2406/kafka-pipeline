import io
import json
import uuid
from datetime import datetime, timezone

import fastavro
import pytest
from kafka import KafkaConsumer

from consumers.storage_consumer import _Buffer, _EXTRACTORS
from model.minio_store import MinioStore
from schemas.message import build_envelope
from tests.conftest import MINIO_BUCKET, TEST_SYMBOL

TOPIC      = "stock.price.realtime"
FIXED_DATE = "2000-01-01"
FIXED_TS   = f"{FIXED_DATE}T08:00:00+00:00"
FIXED_PREFIX = f"price.snapshot/symbol={TEST_SYMBOL}/year=2000/month=01/day=01/"


def _price_msg(price: float = 12345.0) -> dict:
    return build_envelope(
        "price.snapshot", TEST_SYMBOL, "HOSE",
        {"price": price, "change": 100.0, "pct_change": 0.81,
         "volume": 500_000, "bid": 12300.0, "ask": 12400.0},
        timestamp=FIXED_TS,
    )


def _read_avro(minio_client, prefix: str) -> list[dict]:
    """Download the first Avro file under prefix and return records as a list of dicts."""
    objects = list(minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
    assert objects, f"No Avro file found under s3://{MINIO_BUCKET}/{prefix}"
    response = minio_client.get_object(MINIO_BUCKET, objects[0].object_name)
    try:
        return list(fastavro.reader(io.BytesIO(response.read())))
    finally:
        response.close()
        response.release_conn()


# ── tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.integration
def test_price_snapshot_written_to_minio(minio_client):
    """_Buffer should write an Avro file and the row should be readable back."""
    msg = _price_msg(price=12345.0)
    buf = _Buffer(MinioStore(MINIO_BUCKET, client=minio_client))
    buf.add(msg)
    buf.flush()

    records = _read_avro(minio_client, FIXED_PREFIX)
    row = next(r for r in records if r["symbol"] == TEST_SYMBOL)
    assert float(row["price"]) == 12345.0
    assert row["exchange"] == "HOSE"


@pytest.mark.integration
def test_avro_partition_path_structure(minio_client):
    """Avro files must be stored under the correct year/month/day partition prefix."""
    buf = _Buffer(MinioStore(MINIO_BUCKET, client=minio_client))
    buf.add(_price_msg())
    buf.flush()

    objects = list(minio_client.list_objects(MINIO_BUCKET, prefix=FIXED_PREFIX, recursive=True))
    assert objects, f"Expected objects under {FIXED_PREFIX}"
    assert objects[0].object_name.endswith(".avro")


@pytest.mark.integration
def test_extractor_produces_correct_fields():
    """_EXTRACTORS must map all envelope fields to the Avro record schema."""
    msg = _price_msg(price=99999.0)
    row = _EXTRACTORS["price.snapshot"](msg)
    assert row["price"] == 99999.0
    assert row["symbol"] == TEST_SYMBOL
    assert row["exchange"] == "HOSE"
    assert row["time"] == datetime.fromisoformat(FIXED_TS).astimezone(timezone.utc)


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

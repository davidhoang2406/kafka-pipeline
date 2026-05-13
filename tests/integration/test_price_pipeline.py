import json
import uuid

import psycopg2.extras
import pytest
from kafka import KafkaConsumer

from consumers.storage_consumer import _ROUTES
from schemas.message import build_envelope
from tests.conftest import TEST_SYMBOL

TOPIC = "stock.price.realtime"
FIXED_TS_1 = "2000-01-01T08:00:00+00:00"
FIXED_TS_2 = "2000-01-02T08:00:00+00:00"


def _price_msg(ts: str, price: float = 12345.0) -> dict:
    return build_envelope(
        "price.snapshot", TEST_SYMBOL, "HOSE",
        {"price": price, "change": 100.0, "pct_change": 0.81,
         "volume": 500_000, "bid": 12300.0, "ask": 12400.0},
        timestamp=ts,
    )


def _insert(conn, msg: dict) -> None:
    sql, extractor = _ROUTES["price.snapshot"]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, [extractor(msg)])
    conn.commit()


# ── tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.integration
def test_price_snapshot_round_trip(db_conn):
    msg = _price_msg(FIXED_TS_1, price=12345.0)
    _insert(db_conn, msg)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT price, symbol FROM price_snapshots WHERE symbol = %s AND time = %s",
            (TEST_SYMBOL, FIXED_TS_1),
        )
        row = cur.fetchone()

    assert row is not None, "Row not found in price_snapshots after insert"
    assert float(row[0]) == 12345.0
    assert row[1] == TEST_SYMBOL


@pytest.mark.integration
def test_duplicate_message_idempotent(db_conn):
    msg = _price_msg(FIXED_TS_2)
    _insert(db_conn, msg)
    _insert(db_conn, msg)   # exact duplicate

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM price_snapshots WHERE symbol = %s AND time = %s",
            (TEST_SYMBOL, FIXED_TS_2),
        )
        count = cur.fetchone()[0]

    assert count == 1, "ON CONFLICT DO NOTHING should prevent duplicate rows"


@pytest.mark.integration
def test_consumer_group_isolation(kafka_producer, kafka_bootstrap):
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

import json
import os
import socket
import uuid

import psycopg2
import pytest
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TIMESCALE_URL   = os.getenv("TIMESCALE_URL", "postgresql://postgres:password@localhost:5432/stocks")
TEST_SYMBOL     = "__TEST__"


# ── helpers ───────────────────────────────────────────────────────────────────

def _kafka_reachable() -> bool:
    try:
        host, port = KAFKA_BOOTSTRAP.split(":")
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except OSError:
        return False


def _db_reachable() -> bool:
    try:
        conn = psycopg2.connect(TIMESCALE_URL)
        conn.close()
        return True
    except Exception:
        return False


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def kafka_bootstrap():
    if not _kafka_reachable():
        pytest.skip("Kafka not reachable — run `docker compose up -d` first")
    return KAFKA_BOOTSTRAP


@pytest.fixture
def kafka_producer(kafka_bootstrap):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode() if k else None,
        acks=1,
    )
    yield producer
    producer.flush()
    producer.close()


@pytest.fixture
def db_conn():
    if not _db_reachable():
        pytest.skip("TimescaleDB not reachable — run `docker compose up -d` first")
    conn = psycopg2.connect(TIMESCALE_URL)
    yield conn
    with conn.cursor() as cur:
        cur.execute("DELETE FROM price_snapshots WHERE symbol = %s", (TEST_SYMBOL,))
        cur.execute("DELETE FROM ohlcv_daily    WHERE symbol = %s", (TEST_SYMBOL,))
        cur.execute("DELETE FROM financials      WHERE symbol = %s", (TEST_SYMBOL,))
    conn.commit()
    conn.close()


@pytest.fixture
def unique_group():
    """Fresh consumer group ID per test — no committed offsets."""
    return f"test-{uuid.uuid4().hex[:12]}"

import json
import os
import socket
import uuid

import pytest
from kafka import KafkaProducer
from minio import Minio

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",    "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",       "market-data")
TEST_SYMBOL      = "__TEST__"


# ── helpers ───────────────────────────────────────────────────────────────────

def _kafka_reachable() -> bool:
    try:
        host, port = KAFKA_BOOTSTRAP.split(":")
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except OSError:
        return False


def _minio_reachable() -> bool:
    try:
        import urllib.request
        urllib.request.urlopen(f"{MINIO_ENDPOINT}/minio/health/live", timeout=2)
        return True
    except Exception:
        return False


def _make_minio_client() -> Minio:
    secure = MINIO_ENDPOINT.startswith("https://")
    host   = MINIO_ENDPOINT.split("://", 1)[-1]
    return Minio(host, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=secure)


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
def minio_client():
    if not _minio_reachable():
        pytest.skip("MinIO not reachable — run `docker compose up -d` first")
    client = _make_minio_client()
    yield client
    # Cleanup: delete all test objects written by this test run
    for obj in client.list_objects(MINIO_BUCKET, recursive=True):
        if f"symbol={TEST_SYMBOL}" in obj.object_name:
            client.remove_object(MINIO_BUCKET, obj.object_name)


@pytest.fixture
def unique_group():
    """Fresh consumer group ID per test — no committed offsets."""
    return f"test-{uuid.uuid4().hex[:12]}"

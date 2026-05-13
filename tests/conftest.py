import json
import os
import socket
import uuid

import boto3
import pytest
from kafka import KafkaProducer

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
def s3_client():
    if not _minio_reachable():
        pytest.skip("MinIO not reachable — run `docker compose up -d` first")
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    yield client
    # Cleanup: delete all test objects written by this test run
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=MINIO_BUCKET):
        for obj in page.get("Contents", []):
            if f"symbol={TEST_SYMBOL}" in obj["Key"]:
                client.delete_object(Bucket=MINIO_BUCKET, Key=obj["Key"])


@pytest.fixture
def unique_group():
    """Fresh consumer group ID per test — no committed offsets."""
    return f"test-{uuid.uuid4().hex[:12]}"

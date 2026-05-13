"""
Integration tests for the alert path:
  produce message → Kafka → consume → _check() → assert output.

These tests exercise the full Kafka produce/consume cycle; the alert
rule evaluation itself is covered thoroughly in test_alert_rules.py.
"""
import json
import uuid

import pytest
from kafka import KafkaConsumer

from consumers.alert_consumer import _check, _load_rules
from schemas.message import build_envelope

TOPIC = "stock.price.realtime"


def _produce_and_consume(kafka_producer, kafka_bootstrap, unique_group, pct_change: float) -> dict | None:
    """Send one price snapshot and return the first matching message consumed."""
    test_id = str(uuid.uuid4())
    msg = build_envelope(
        "price.snapshot", "VCB", "HOSE",
        {"price": 85000.0, "pct_change": pct_change,
         "change": 0.0, "volume": 0, "bid": 0.0, "ask": 0.0,
         "_test_id": test_id},
    )
    kafka_producer.send(TOPIC, value=msg, key="VCB")
    kafka_producer.flush()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=kafka_bootstrap,
        group_id=unique_group,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode()),
        consumer_timeout_ms=8000,
    )
    result = None
    for record in consumer:
        if record.value.get("payload", {}).get("_test_id") == test_id:
            result = record.value
            break
    consumer.close()
    return result


@pytest.mark.integration
def test_alert_fires_on_threshold(kafka_producer, kafka_bootstrap, unique_group, capsys):
    received = _produce_and_consume(kafka_producer, kafka_bootstrap, unique_group, pct_change=-4.0)
    assert received is not None, "Test message was not consumed from Kafka"

    rules = _load_rules()
    _check(rules, received["symbol"], received["payload"])

    assert "[ALERT" in capsys.readouterr().out


@pytest.mark.integration
def test_alert_silent_below_threshold(kafka_producer, kafka_bootstrap, unique_group, capsys):
    received = _produce_and_consume(kafka_producer, kafka_bootstrap, unique_group, pct_change=-1.0)
    assert received is not None, "Test message was not consumed from Kafka"

    rules = _load_rules()
    _check(rules, received["symbol"], received["payload"])

    assert capsys.readouterr().out == ""

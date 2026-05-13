# Shared KafkaConsumer wrapper used by all consumers.
# Handles broker connection, JSON deserialization, and offset management.
# Use as a context manager: `with BaseConsumer(topics, group_id) as c: c.messages()`.
import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()


class BaseConsumer:
    def __init__(self, topics: list, group_id: str, auto_offset_reset: str = "earliest"):
        self._consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            enable_auto_commit=True,
        )

    def messages(self):
        """Blocking iterator — yields one message at a time."""
        return self._consumer

    def poll(self, timeout_ms: int = 1000) -> list:
        """Non-blocking poll — returns a flat list of messages, empty list on timeout."""
        records = self._consumer.poll(timeout_ms=timeout_ms)
        return [msg for batch in records.values() for msg in batch]

    def close(self):
        self._consumer.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

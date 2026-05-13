import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()


class BaseProducer:
    def __init__(self):
        self._producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,
        )

    def send(self, topic: str, value: dict, key: str = None):
        self._producer.send(topic, value=value, key=key)

    def flush(self):
        self._producer.flush()

    def close(self):
        self._producer.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

"""
Centralised MinIO wrapper for all pipeline operations.

Usage
-----
    from model.minio_store import MinioStore

    store = MinioStore("market-data")          # client built from env vars
    store.ensure_bucket()                      # idempotent create
    store.set_expiry_lifecycle(days=30)        # lifecycle rule
    store.write_partitioned("price.snapshot", "VCB", rows, schema)
    store.write_avro("custom/key.avro", schema, rows)
    n = store.flush_all()                      # delete every object
"""
import io
import logging
import os
import time

import fastavro
from minio import Minio
from minio.lifecycleconfig import Expiration, Filter, LifecycleConfig, Rule

log = logging.getLogger(__name__)


def _build_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    secure   = endpoint.startswith("https://")
    host     = endpoint.split("://", 1)[-1]
    return Minio(
        host,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=secure,
    )


class MinioStore:
    """Thin wrapper around the MinIO client for common pipeline operations."""

    def __init__(self, bucket: str, client: Minio | None = None) -> None:
        self.bucket  = bucket
        self._client = client or _build_client()

    # ── Bucket management ──────────────────────────────────────────────────────

    def ensure_bucket(self) -> None:
        """Create the bucket if it does not already exist."""
        if self._client.bucket_exists(self.bucket):
            log.debug("bucket '%s' already exists", self.bucket)
        else:
            self._client.make_bucket(self.bucket)
            log.info("bucket '%s' created", self.bucket)

    def set_expiry_lifecycle(self, days: int) -> None:
        """Apply an object-expiry lifecycle rule (replaces any existing rule)."""
        config = LifecycleConfig([
            Rule(
                "Enabled",
                rule_filter=Filter(prefix=""),
                rule_id=f"expire-after-{days}-days",
                expiration=Expiration(days=days),
            )
        ])
        self._client.set_bucket_lifecycle(self.bucket, config)
        log.info("lifecycle set on '%s': objects expire after %d days", self.bucket, days)

    # ── Object writes ──────────────────────────────────────────────────────────

    def write_avro(self, key: str, schema, rows: list[dict]) -> None:
        """Serialize rows as deflate-compressed Avro and upload to the exact key."""
        if not rows:
            return
        buf = io.BytesIO()
        fastavro.writer(buf, schema, rows, codec="deflate")
        data = buf.getvalue()
        self._client.put_object(
            self.bucket, key, io.BytesIO(data), len(data),
            content_type="avro/binary",
        )
        log.info("wrote %d rows → s3://%s/%s", len(rows), self.bucket, key)

    def write_partitioned(
        self,
        event_type: str,
        symbol: str,
        rows: list[dict],
        schema,
    ) -> None:
        """Write rows using the standard partition layout, deriving the date from rows[0]['time'].

        Layout: {event_type}/symbol={symbol}/year={Y}/month={m}/day={d}/part-{ts_ms}.avro
        Slashes in symbol are replaced with dashes (e.g. BTC/USDT → BTC-USDT).
        """
        if not rows:
            return
        date_str             = rows[0]["time"][:10]
        year, month, day     = date_str[:4], date_str[5:7], date_str[8:10]
        safe_symbol          = symbol.replace("/", "-")
        ts_ms                = int(time.time() * 1000)
        key = (f"{event_type}/symbol={safe_symbol}"
               f"/year={year}/month={month}/day={day}/part-{ts_ms}.avro")
        self.write_avro(key, schema, rows)

    # ── Object reads / deletes ─────────────────────────────────────────────────

    def list_objects(self, prefix: str = "", recursive: bool = True):
        """Return an iterable of MinIO object info for the given prefix."""
        return self._client.list_objects(self.bucket, prefix=prefix, recursive=recursive)

    def get_object(self, key: str):
        """Return a streaming HTTP response for the given object key."""
        return self._client.get_object(self.bucket, key)

    def delete_object(self, key: str) -> None:
        """Delete a single object by key."""
        self._client.remove_object(self.bucket, key)

    def flush_all(self, prefix: str = "") -> int:
        """Delete all objects in the bucket (optionally scoped to a prefix).

        Returns the number of objects deleted.
        """
        objs = list(self.list_objects(prefix=prefix))
        for obj in objs:
            self.delete_object(obj.object_name)
        log.info("deleted %d objects from s3://%s/%s*", len(objs), self.bucket, prefix)
        return len(objs)

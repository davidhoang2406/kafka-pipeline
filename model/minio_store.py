"""
Centralised MinIO wrapper for all pipeline operations.

Usage
-----
    from model.minio_store import MinioStore

    store = MinioStore("market-data")          # client built from env vars
    store.ensure_bucket()                      # idempotent create
    store.set_expiry_lifecycle(days=30)        # lifecycle rule
    store.write_avro("custom/key.avro", schema, rows)
    n = store.flush_all()                      # delete every object
"""
import io
import logging
import os
import time

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.deleteobjects import DeleteObject
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule

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
                rule_filter=None,
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

    def write_text(self, key: str, text: str) -> None:
        """Upload a UTF-8 text string to the exact key."""
        data = text.encode()
        self._client.put_object(
            self.bucket, key, io.BytesIO(data), len(data),
            content_type="text/plain; charset=utf-8",
        )
        log.info("wrote text → s3://%s/%s", self.bucket, key)

    def write_parquet(self, key: str, schema: pa.Schema, rows: list[dict]) -> None:
        """Serialize rows as Snappy-compressed Parquet and upload to the exact key."""
        if not rows:
            return
        table = pa.Table.from_pylist(rows, schema=schema)
        buf   = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        data  = buf.getvalue()
        self._client.put_object(
            self.bucket, key, io.BytesIO(data), len(data),
            content_type="application/octet-stream",
        )
        log.info("wrote %d rows → s3://%s/%s", len(rows), self.bucket, key)

    # ── Object reads / deletes ─────────────────────────────────────────────────

    def list_objects(self, prefix: str = "", recursive: bool = True):
        """Return an iterable of MinIO object info for the given prefix."""
        return self._client.list_objects(self.bucket, prefix=prefix, recursive=recursive)

    def get_object(self, key: str):
        """Return a streaming HTTP response for the given object key."""
        return self._client.get_object(self.bucket, key)

    def download_file(self, key: str, local_path: str) -> None:
        """Download an object to a local file path."""
        self._client.fget_object(self.bucket, key, local_path)

    def read_avro(self, key: str) -> list[dict]:
        """Download an Avro file by key and return all records as a list of dicts."""
        response = self.get_object(key)
        try:
            return list(fastavro.reader(io.BytesIO(response.read())))
        finally:
            response.close()
            response.release_conn()

    def delete_object(self, key: str) -> None:
        """Delete a single object by key."""
        self._client.remove_object(self.bucket, key)

    def flush_all(self, prefix: str = "") -> int:
        """Delete all objects in the bucket (optionally scoped to a prefix).

        Returns the number of objects deleted.
        """
        objs = list(self.list_objects(prefix=prefix))
        errors = list(self._client.remove_objects(
            self.bucket,
            (DeleteObject(obj.object_name) for obj in objs),
        ))
        if errors:
            for err in errors:
                log.error("delete failed: %s", err)
        log.info("deleted %d objects from s3://%s/%s*", len(objs), self.bucket, prefix)
        return len(objs)

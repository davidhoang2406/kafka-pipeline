# Creates MinIO buckets and applies lifecycle rules. Safe to re-run — idempotent.
#   market-data     — raw streaming data (price snapshots, financials); 30-day expiry
#   market-analysis — batch OHLCV bars (stock + crypto); no expiry (kept indefinitely)
import os

from dotenv import load_dotenv
from minio import Minio
from minio.lifecycleconfig import Expiration, Filter, LifecycleConfig, Rule

load_dotenv()

RAW_BUCKET      = os.getenv("MINIO_BUCKET", "market-data")
ANALYSIS_BUCKET = os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis")
RETENTION_DAYS  = 30


def _make_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    secure   = endpoint.startswith("https://")
    host     = endpoint.split("://", 1)[-1]
    return Minio(
        host,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=secure,
    )


def _ensure_bucket(client: Minio, bucket: str) -> None:
    if client.bucket_exists(bucket):
        print(f"Bucket '{bucket}' already exists.")
    else:
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' created.")


def run() -> None:
    client = _make_client()

    _ensure_bucket(client, RAW_BUCKET)
    lifecycle = LifecycleConfig(
        [
            Rule(
                "Enabled",
                rule_filter=Filter(prefix=""),
                rule_id="expire-after-30-days",
                expiration=Expiration(days=RETENTION_DAYS),
            ),
        ]
    )
    client.set_bucket_lifecycle(RAW_BUCKET, lifecycle)
    print(f"Lifecycle rule set on '{RAW_BUCKET}': objects expire after {RETENTION_DAYS} days.")

    _ensure_bucket(client, ANALYSIS_BUCKET)
    print(f"No expiry on '{ANALYSIS_BUCKET}' — OHLCV bars are kept indefinitely.")


if __name__ == "__main__":
    run()

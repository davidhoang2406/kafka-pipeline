# Creates the market-data bucket in MinIO and applies a 30-day expiry lifecycle rule.
# Safe to re-run — bucket creation and lifecycle config are both idempotent.
import os

from dotenv import load_dotenv
from minio import Minio
from minio.lifecycleconfig import Expiration, Filter, LifecycleConfig, Rule

load_dotenv()

BUCKET          = os.getenv("MINIO_BUCKET", "market-data")
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


def run() -> None:
    client = _make_client()

    if client.bucket_exists(BUCKET):
        print(f"Bucket '{BUCKET}' already exists.")
    else:
        client.make_bucket(BUCKET)
        print(f"Bucket '{BUCKET}' created.")

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
    client.set_bucket_lifecycle(BUCKET, lifecycle)
    print(f"Lifecycle rule set: objects expire after {RETENTION_DAYS} days.")


if __name__ == "__main__":
    run()

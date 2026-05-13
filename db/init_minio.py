# Creates the market-data bucket in MinIO (safe to re-run).
# Run once after `docker compose up -d minio` to initialize storage.
import os

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

BUCKET = os.getenv("MINIO_BUCKET", "market-data")


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
        print(f"Bucket '{BUCKET}' already exists — nothing to do.")
    else:
        client.make_bucket(BUCKET)
        print(f"Bucket '{BUCKET}' created.")


if __name__ == "__main__":
    run()

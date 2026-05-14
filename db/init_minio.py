# Creates MinIO buckets and applies lifecycle rules. Safe to re-run — idempotent.
#   market-data     — raw streaming data (price snapshots); 30-day expiry
#   market-analysis — batch OHLCV bars (stock + crypto); no expiry
import os

from dotenv import load_dotenv

from model.minio_store import MinioStore

load_dotenv()

RETENTION_DAYS = 30


def run() -> None:
    raw      = MinioStore(os.getenv("MINIO_BUCKET", "market-data"))
    analysis = MinioStore(os.getenv("MINIO_ANALYSIS_BUCKET", "market-analysis"))

    raw.ensure_bucket()
    raw.set_expiry_lifecycle(RETENTION_DAYS)
    print(f"Bucket '{raw.bucket}' ready — objects expire after {RETENTION_DAYS} days.")

    analysis.ensure_bucket()
    print(f"Bucket '{analysis.bucket}' ready — no expiry (OHLCV bars kept indefinitely).")


if __name__ == "__main__":
    run()

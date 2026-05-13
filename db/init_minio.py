# Creates the market-data bucket in MinIO (safe to re-run).
# Run once after `docker compose up -d minio` to initialise storage.
import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("MINIO_BUCKET", "market-data")


def run() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' created.")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
            print(f"Bucket '{BUCKET}' already exists — nothing to do.")
        else:
            raise


if __name__ == "__main__":
    run()

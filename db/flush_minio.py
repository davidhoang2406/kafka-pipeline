# Deletes all Parquet objects from the market-data bucket (irreversible).
# Called by `make storage-flush` after the user confirms the operation.
import os

from dotenv import load_dotenv
from minio import Minio

load_dotenv()


def run() -> None:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    secure   = endpoint.startswith("https://")
    host     = endpoint.split("://", 1)[-1]
    client   = Minio(
        host,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=secure,
    )
    bucket = os.getenv("MINIO_BUCKET", "market-data")
    objs   = list(client.list_objects(bucket, recursive=True))
    for obj in objs:
        client.remove_object(bucket, obj.object_name)
    print(f"Deleted {len(objs)} objects from {bucket}.")


if __name__ == "__main__":
    run()

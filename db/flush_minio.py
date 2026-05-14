# Deletes all objects from the market-data bucket (irreversible).
# Called by `make storage-flush` after the user confirms the operation.
import os

from dotenv import load_dotenv

from model.minio_store import MinioStore

load_dotenv()


def run() -> None:
    store = MinioStore(os.getenv("MINIO_BUCKET", "market-data"))
    n     = store.flush_all()
    print(f"Deleted {n} objects from '{store.bucket}'.")


if __name__ == "__main__":
    run()

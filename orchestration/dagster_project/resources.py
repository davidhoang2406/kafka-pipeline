import logging
import subprocess

from dagster import ConfigurableResource
from minio import Minio

log = logging.getLogger(__name__)


class MinioResource(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    market_data_bucket: str
    market_analysis_bucket: str

    def _client(self) -> Minio:
        secure = self.endpoint.startswith("https://")
        host   = self.endpoint.split("://", 1)[-1]
        return Minio(host, access_key=self.access_key, secret_key=self.secret_key, secure=secure)

    def partition_exists(self, bucket: str, prefix: str) -> bool:
        """Return True if at least one object exists under the given MinIO prefix."""
        try:
            return any(True for _ in self._client().list_objects(bucket, prefix=prefix, recursive=False))
        except Exception:
            return False


class SparkClusterResource(ConfigurableResource):
    """Submits batch jobs via `docker exec` into the running spark-master container.

    Keeps the Dagster image lightweight (no PySpark required) and reuses the
    existing Spark cluster that docker-compose already manages.
    """
    container_name: str = "spark-master"

    def submit(self, args: list[str]) -> None:
        cmd = ["docker", "exec", self.container_name, "python", "/opt/project/main.py"] + args
        log.info("Submitting: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.stdout:
            log.info(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(
                f"Spark job failed (exit {result.returncode}):\n{result.stderr}"
            )

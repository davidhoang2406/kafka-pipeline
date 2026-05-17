import logging

import docker as docker_sdk
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
        cmd = ["python", "/opt/project/main.py"] + args
        log.info("Submitting to %s: %s", self.container_name, " ".join(cmd))
        client    = docker_sdk.from_env()
        container = client.containers.get(self.container_name)
        exit_code, output = container.exec_run(cmd, demux=False)
        if output:
            log.info(output.decode(errors="replace"))
        if exit_code != 0:
            raise RuntimeError(
                f"Spark job failed (exit {exit_code}):\n{output.decode(errors='replace') if output else ''}"
            )

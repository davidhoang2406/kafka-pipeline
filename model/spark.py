"""
Shared SparkSession wrapper for all batch jobs.

Usage
-----
    from model.spark import SparkFactory

    # As a context manager (recommended — stops session automatically):
    with SparkFactory("my_job") as spark:
        df = spark.read.format("avro").load("s3a://market-data/...")

    # Or manually:
    factory = SparkFactory("my_job")
    spark = factory.session
    # ...
    factory.stop()

Master resolution (SPARK_MASTER_URL env var):
  - Not set / local dev  → local[*]  (in-process, no cluster needed)
  - Docker               → spark://spark-master:7077

S3A is pre-configured to talk to the MinIO instance defined by env vars
(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY). In Docker, these
are set to point at the minio container (http://minio:9000) via
docker-compose, so jobs read/write s3a://<bucket>/... without changes.

S3A JARs are pre-baked into the spark.Dockerfile — no runtime download.
"""
import os

from pyspark.sql import SparkSession


class SparkFactory:
    """Wraps SparkSession creation with S3A/MinIO config pre-wired.

    Implements the context manager protocol so sessions are always stopped
    on exit, even if the job raises an exception.
    """

    def __init__(self, app_name: str) -> None:
        self._app_name = app_name
        self._session: SparkSession | None = None

    @property
    def session(self) -> SparkSession:
        if self._session is None:
            self._session = self._build()
        return self._session

    def stop(self) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None

    def __enter__(self) -> SparkSession:
        return self.session

    def __exit__(self, *_) -> None:
        self.stop()

    def _build(self) -> SparkSession:
        # SPARK_MASTER_URL is set in docker-compose for the cluster;
        # falls back to local[*] for local dev without a cluster.
        master = os.getenv("SPARK_MASTER_URL", "local[*]")
        spark = (SparkSession.builder
                 .appName(self._app_name)
                 .master(master)
                 # S3A JARs are pre-baked in spark.Dockerfile — no download needed
                 .config("spark.hadoop.fs.s3a.endpoint",          os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
                 .config("spark.hadoop.fs.s3a.access.key",        os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
                 .config("spark.hadoop.fs.s3a.secret.key",        os.getenv("MINIO_SECRET_KEY", "minioadmin"))
                 .config("spark.hadoop.fs.s3a.path.style.access", "true")  # required for MinIO
                 .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
                 # Event logging — feeds the Spark History Server at http://localhost:18080
                 .config("spark.eventLog.enabled", "true")
                 .config("spark.eventLog.dir",     "/tmp/spark-events")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        return spark

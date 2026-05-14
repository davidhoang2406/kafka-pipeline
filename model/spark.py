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

S3A is pre-configured to talk to the MinIO instance defined by env vars
(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY), so any batch job
can immediately read/write s3a://<bucket>/... without extra setup.
"""
import os

from pyspark.sql import SparkSession


class SparkFactory:
    """Wraps SparkSession creation with S3A/MinIO config pre-wired.

    Implements the context manager protocol so sessions are always stopped
    on exit, even if the job raises an exception.
    """

    def __init__(self, app_name: str, master: str = "local[*]") -> None:
        self._app_name = app_name
        self._master   = master
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
        spark = (SparkSession.builder
                 .appName(self._app_name)
                 .master(self._master)
                 # S3A connector — downloaded once by Spark's package resolver, then cached
                 .config("spark.jars.packages",
                         "org.apache.hadoop:hadoop-aws:3.3.4,"
                         "com.amazonaws:aws-java-sdk-bundle:1.12.262")
                 # MinIO S3A config
                 .config("spark.hadoop.fs.s3a.endpoint",          os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
                 .config("spark.hadoop.fs.s3a.access.key",        os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
                 .config("spark.hadoop.fs.s3a.secret.key",        os.getenv("MINIO_SECRET_KEY", "minioadmin"))
                 .config("spark.hadoop.fs.s3a.path.style.access", "true")  # required for MinIO
                 .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        return spark

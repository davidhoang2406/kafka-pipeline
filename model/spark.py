"""
Shared SparkSession factory for all batch jobs.

Usage
-----
    from model.spark import build_spark

    spark = build_spark("my_job")
    spark.sparkContext.setLogLevel("WARN")
    # ... use spark ...
    spark.stop()

S3A is pre-configured to talk to the MinIO instance defined by env vars
(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY), so any batch job
can immediately read/write s3a://<bucket>/... without extra setup.
"""
import os

from pyspark.sql import SparkSession


def build_spark(app_name: str, master: str = "local[*]") -> SparkSession:
    """Return a SparkSession with S3A wired to the local MinIO instance."""
    return (SparkSession.builder
            .appName(app_name)
            .master(master)
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

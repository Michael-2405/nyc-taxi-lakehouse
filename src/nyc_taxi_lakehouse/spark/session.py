from __future__ import annotations

import structlog
from pyspark.sql import SparkSession

from nyc_taxi_lakehouse.config.settings import settings

logger = structlog.get_logger(__name__)

HADOOP_AWS_VERSION = "3.5.0"

def create_spark_session(app_name: str) -> SparkSession:
  protocol = "https" if settings.minio.secure else "http"
  endpoint = f"{protocol}://{settings.minio.endpoint}"

  logger.info("Creating Spark session", app_name=app_name, minio_endpoint=endpoint)

  spark = (
    SparkSession.builder
    .appName(app_name)
    .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}")
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.access.key", settings.minio.root_user)
    .config("spark.hadoop.fs.s3a.secret.key", settings.minio.root_password)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
  )

  return spark

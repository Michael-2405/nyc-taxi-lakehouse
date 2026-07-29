from __future__ import annotations

from minio.xml import B
import structlog

from nyc_taxi_lakehouse.config.logging import configure_logging
from nyc_taxi_lakehouse.config.settings import settings
from nyc_taxi_lakehouse.download.manifest import build_manifest
from nyc_taxi_lakehouse.spark.jobs.raw_ingest import run_raw_ingest
from nyc_taxi_lakehouse.spark.session import create_spark_session
from nyc_taxi_lakehouse.storage.client import StorageClient

logger = structlog.get_logger(__name__)

def main() -> None:
  configure_logging()

  logger.info("Raw ingestion batch started")

  manifest = build_manifest(start_year=2023, start_month=1, end_year=2025, end_month=12)

  storage = StorageClient()
  spark = create_spark_session("raw-ingest-batch")

  for dataset in manifest:
    success_marker = f"{settings.minio.bronze_prefix}/{dataset.object_name}/_SUCCESS"

    if storage.object_exists(settings.minio.bucket_name, success_marker):
      logger.info("Skipping dataset, already in bronze", filename=dataset.filename)
      continue

    run_raw_ingest(
      spark=spark,
      dataset=dataset,
      bucket_name=settings.minio.bucket_name,
      raw_prefix=settings.minio.raw_prefix,
      bronze_prefix=settings.minio.bronze_prefix
    )

  spark.stop()

  logger.info("Raw ingestion batch finished")

if __name__ == "__main__":
  main()

from __future__ import annotations

import structlog

from nyc_taxi_lakehouse.config.logging import configure_logging
from nyc_taxi_lakehouse.config.settings import settings
from nyc_taxi_lakehouse.download.manifest import build_manifest
from nyc_taxi_lakehouse.spark.jobs.transform_silver import run_transform_silver
from nyc_taxi_lakehouse.spark.session import create_spark_session
from nyc_taxi_lakehouse.spark.transformations.enrichment import load_zone_lookup
from nyc_taxi_lakehouse.storage.client import StorageClient

logger = structlog.get_logger(__name__)

def main() -> None:
  configure_logging()

  logger.info("Silver transformation batch started")

  manifest = build_manifest(start_year=2023, start_month=1, end_year=2025, end_month=12)

  storage = StorageClient()
  spark = create_spark_session("transform-silver-batch")

  lookup_df = load_zone_lookup(
    spark,
    bucket_name=settings.minio.bucket_name,
    reference_prefix=settings.minio.reference_prefix
  )

  for dataset in manifest:
    success_marker = f"{settings.minio.silver_prefix}/{dataset.object_name}/_SUCCESS"

    if storage.object_exists(settings.minio.bucket_name, success_marker):
      logger.info("Skipping dataset, already in silver", filename=dataset.object_name)
      continue

    run_transform_silver(
      spark=spark,
      dataset=dataset,
      lookup_df=lookup_df,
      bucket_name=settings.minio.bucket_name,
      bronze_prefix=settings.minio.bronze_prefix,
      silver_prefix=settings.minio.silver_prefix
    )

  spark.stop()

  logger.info("Silver transformation batch finished")

if __name__ == "__main__":
  main()

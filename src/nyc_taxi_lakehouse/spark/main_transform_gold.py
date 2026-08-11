from __future__ import annotations

import structlog

from nyc_taxi_lakehouse.config.logging import configure_logging
from nyc_taxi_lakehouse.config.settings import settings
from nyc_taxi_lakehouse.download.manifest import build_manifest
from nyc_taxi_lakehouse.spark.jobs.transform_gold import run_transform_gold
from nyc_taxi_lakehouse.spark.session import create_spark_session
from nyc_taxi_lakehouse.storage.client import StorageClient

logger = structlog.get_logger(__name__)


def main() -> None:
    configure_logging()
    logger.info("Gold aggregation batch started")

    manifest = build_manifest(start_year=2023, start_month=1, end_year=2025, end_month=12)

    storage = StorageClient()
    spark = create_spark_session("transform-gold-batch")

    for dataset in manifest:
        success_marker = f"{settings.minio.gold_prefix}/{dataset.object_name}/_SUCCESS"

        if storage.object_exists(settings.minio.bucket_name, success_marker):
            logger.info("Skipping dataset, already in gold", filename=dataset.filename)
            continue

        run_transform_gold(
            spark=spark,
            dataset=dataset,
            bucket_name=settings.minio.bucket_name,
            silver_prefix=settings.minio.silver_prefix,
            gold_prefix=settings.minio.gold_prefix,
        )

    spark.stop()
    logger.info("Gold aggregation batch finished")


if __name__ == "__main__":
    main()
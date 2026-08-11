from __future__ import annotations

import structlog

from nyc_taxi_lakehouse.config.logging import configure_logging
from nyc_taxi_lakehouse.config.settings import settings
from nyc_taxi_lakehouse.download.manifest import build_manifest
from nyc_taxi_lakehouse.spark.jobs.validate_silver import run_validate_silver
from nyc_taxi_lakehouse.spark.session import create_spark_session

logger = structlog.get_logger(__name__)

def main() -> None:
    configure_logging()

    logger.info("Silver validation batch start")

    manifest = build_manifest(start_year=2023, start_month=1, end_year=2025, end_month=12)

    spark = create_spark_session("validate-silver-batch")

    for dataset in manifest:
        run_validate_silver(
            spark=spark,
            dataset=dataset,
            bucket_name=settings.minio.bucket_name,
            silver_prefix=settings.minio.silver_prefix,
        )

    spark.stop()

    logger.info("Silver validation batch end")

if __name__ == "__main__":
    main()
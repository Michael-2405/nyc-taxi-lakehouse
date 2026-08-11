from __future__ import annotations

import structlog
from pyspark.sql import SparkSession

from nyc_taxi_lakehouse.download.manifest import DatasetFile
from nyc_taxi_lakehouse.spark.quality.silver_validation import validate_silver_dataframe

logger = structlog.get_logger(__name__)

def run_validate_silver(spark: SparkSession, dataset: DatasetFile, bucket_name: str, silver_prefix: str) -> None:
    source_path = f"s3a://{bucket_name}/{silver_prefix}/{dataset.object_name}"

    logger.info("Starting silver validation job", source=source_path)

    df = spark.read.parquet(source_path)

    result = validate_silver_dataframe(df)

    if result.success:
        logger.info("Silver validation passed", source=source_path)
    else:
        logger.warning("Silver validation failed", source=source_path, errors=result.errors)
from __future__ import annotations

import structlog
from pyspark.sql import SparkSession

from nyc_taxi_lakehouse.download.manifest import DatasetFile
from nyc_taxi_lakehouse.spark.transformations.aggregations import build_daily_aggregates

logger = structlog.get_logger(__name__)


def run_transform_gold(
        spark: SparkSession,
        dataset: DatasetFile,
        bucket_name: str,
        silver_prefix: str,
        gold_prefix: str,
) -> None:
    source_path = f"s3a://{bucket_name}/{silver_prefix}/{dataset.object_name}"
    destination_path = f"s3a://{bucket_name}/{gold_prefix}/{dataset.object_name}"

    logger.info("Starting gold aggregation job", source=source_path, destination=destination_path)

    df = spark.read.parquet(source_path)
    df_gold = build_daily_aggregates(df, year=dataset.year, month=dataset.month)

    df_gold.write.mode("overwrite").parquet(destination_path)

    logger.info(
        "Gold aggregation job completed",
        destination=destination_path,
        row_count=df_gold.count(),
    )
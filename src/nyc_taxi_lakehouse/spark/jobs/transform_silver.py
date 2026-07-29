from __future__ import annotations

import structlog
from pyspark.sql import DataFrame, SparkSession

from nyc_taxi_lakehouse.download.manifest import DatasetFile
from nyc_taxi_lakehouse.spark.transformations.enrichment import enrich_with_zone_lookup
from nyc_taxi_lakehouse.spark.transformations.schema import normalize_schema

logger = structlog.get_logger(__name__)

def run_transform_silver(
    spark: SparkSession,
    dataset: DatasetFile,
    lookup_df: DataFrame,
    bucket_name: str,
    bronze_prefix: str,
    silver_prefix: str,
) -> None:
  source_path = f"s3a://{bucket_name}/{bronze_prefix}/{dataset.object_name}"
  destination_path = f"s3a://{bucket_name}/{silver_prefix}/{dataset.object_name}"

  logger.info(
        "Starting silver transformation job",
        source=source_path,
        destination=destination_path,
    )

  df = spark.read.parquet(source_path)

  df_normalized = normalize_schema(df)
  df_deduplicated = df_normalized.dropDuplicates()
  df_enriched = enrich_with_zone_lookup(df_deduplicated, lookup_df)

  df_enriched.write.mode("overwrite").parquet(destination_path)

  logger.info("Silver transformation job completed", destination=destination_path, row_count=df_enriched.count())

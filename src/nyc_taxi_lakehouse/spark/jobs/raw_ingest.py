from __future__ import annotations
from curses import raw

import structlog
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

from nyc_taxi_lakehouse.download.manifest import DatasetFile

logger = structlog.get_logger(__name__)

def run_raw_ingest(spark: SparkSession, dataset: DatasetFile, bucket_name: str, raw_prefix: str, bronze_prefix: str) -> None:
  source_path = f"s3a://{bucket_name}/{raw_prefix}/{dataset.object_name}"
  destination_path =  f"s3a://{bucket_name}/{bronze_prefix}/{dataset.object_name}"

  logger.info("Starting raw ingestion job", source=source_path, destination=destination_path)

  df = spark.read.parquet(source_path)

  df_with_metadata = df.withColumn(
      "ingestion_timestamp",current_timestamp()
    ).withColumn(
      "source_file", lit(dataset.object_name)
    )

  df_with_metadata.write.mode("overwrite").parquet(destination_path)

  logger.info("Raw ingestion job completed", destination=destination_path, row_count=df_with_metadata.count())

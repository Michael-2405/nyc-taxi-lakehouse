from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def load_zone_lookup(spark, bucket_name: str, reference_prefix: str) -> DataFrame:
  lookup_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3a://{bucket_name}/{reference_prefix}/taxi_zone_lookup.csv")
  )

  return (
    lookup_df
    .withColumnRenamed("LocationID", "location_id")
    .withColumnRenamed("Borough", "borough")
    .withColumnRenamed("Zone", "zone")
    .withColumnRenamed("service_zone", "service_zone")
    .withColumn("location_id", col("location_id").cast("int"))
  )

def enrich_with_zone_lookup(df: DataFrame, lookup_df: DataFrame) -> DataFrame:
  pu_lookup = lookup_df.select(
        col("location_id").alias("pu_location_id"),
        col("borough").alias("pu_borough"),
        col("zone").alias("pu_zone"),
        col("service_zone").alias("pu_service_zone"),
    )

  do_lookup = lookup_df.select(
      col("location_id").alias("do_location_id"),
      col("borough").alias("do_borough"),
      col("zone").alias("do_zone"),
      col("service_zone").alias("do_service_zone"),
  )

  return (
      df
      .join(pu_lookup, on="pu_location_id", how="left")
      .join(do_lookup, on="do_location_id", how="left")
  )

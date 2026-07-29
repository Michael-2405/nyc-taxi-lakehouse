from __future__ import annotations

from h11 import Data
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DecimalType, IntegerType

COLUMN_RENAMES = {
  "VendorID": "vendor_id",
  "RatecodeID": "rate_code_id",
  "PULocationID": "pu_location_id",
  "DOLocationID": "do_location_id",
  "Airport_fee": "airport_fee"
}

INTEGER_COLUMNS = [
  "vendor_id",
  "passenger_count",
  "rate_code_id",
  "pu_location_id",
  "do_location_id",
  "payment_type"
]

DECIMAL_COLUMNS = [
  "fare_amount",
  "extra",
  "mta_tax",
  "tip_amount",
  "tolls_amount",
  "improvement_surcharge",
  "total_amount",
  "congestion_surcharge",
  "airport_fee",
  "cbd_congestion_fee"
]

CANONICAL_COLUMN_ORDER = [
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "ingestion_timestamp",
    "source_file",
]

MONEY_PRECISION = 10
MONEY_SCALE = 2

def normalize_schema(df: DataFrame) -> DataFrame:
  for old_name, new_name in COLUMN_RENAMES.items():
    if old_name in df.columns:
      df = df.withColumnRenamed(old_name, new_name)

  if "cbd_congestion_fee" not in df.columns:
    df = df.withColumn("cbd_congestion_fee", lit(None).cast("double"))

  for column_name in INTEGER_COLUMNS:
    df = df.withColumn(column_name, col(column_name).cast(IntegerType()))

  for column_name in DECIMAL_COLUMNS:
    df = df.withColumn(
      column_name,
      col(column_name).cast(DecimalType(MONEY_PRECISION, MONEY_SCALE))
    )

  return df.select(*CANONICAL_COLUMN_ORDER)

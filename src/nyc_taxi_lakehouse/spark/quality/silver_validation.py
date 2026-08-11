from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pandera.pyspark as pa
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

MIN_EXPECTED_ROWS = 1_000_000
MAX_EXPECTED_ROWS = 6_000_000

@dataclass
class SilverValidationResult:
    success: bool
    errors: list[str] = field(default_factory=list)

def _build_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        columns={
            "vendor_id": pa.Column("int", nullable=False),
            "pu_location_id": pa.Column("int", nullable=False),
            "do_location_id": pa.Column("int", nullable=False),
            "tpep_pickup_datetime": pa.Column(nullable=False),
            "tpep_dropoff_datetime": pa.Column( nullable=False),
        }
    )

def validate_silver_dataframe(df: DataFrame) -> SilverValidationResult:
    errors: list[str] = []
    now = datetime.now()

    schema = _build_schema()
    df_validated = schema.validate(df)
    pandera_errors = df_validated.pandera.errors

    if pandera_errors:
        errors.append(f"Pandera schema errors: {pandera_errors}")

    row_count = df.count()
    if not (MIN_EXPECTED_ROWS <= row_count <= MAX_EXPECTED_ROWS):
        errors.append(
            f"Row count {row_count} outside expected range "
            f"[{MIN_EXPECTED_ROWS}, {MAX_EXPECTED_ROWS}]"
        )

    future_pickups = df.filter(f.col("tpep_pickup_datetime") > now).count()
    if future_pickups > 0:
        errors.append(f"{future_pickups} rows with future tpep_pickup_datetime")

    future_dropoffs = df.filter(f.col("tpep_dropoff_datetime") > now).count()
    if future_dropoffs > 0:
        errors.append(f"{future_dropoffs} rows with future tpep_dropoff_datetime")

    invalid_order = df.filter(
        f.col("tpep_dropoff_datetime") < f.col("tpep_pickup_datetime")
    ).count()
    if invalid_order > 0:
        errors.append(f"{invalid_order} rows with invalid dropoff before pickup")

    return SilverValidationResult(success=len(errors) == 0, errors=errors)
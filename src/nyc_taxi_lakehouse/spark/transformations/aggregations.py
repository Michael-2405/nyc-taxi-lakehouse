from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window


def build_daily_aggregates(df: DataFrame, year: int, month: int) -> DataFrame:
    df = df.filter(
        (f.year("tpep_pickup_datetime") == year)
        & (f.month("tpep_pickup_datetime") == month)
    )

    daily = (
        df.withColumn("trip_date", f.to_date("tpep_pickup_datetime"))
        .groupBy("trip_date")
        .agg(
            f.count("*").alias("total_trips"),
            f.sum("total_amount").alias("total_revenue"),
            f.avg("fare_amount").alias("avg_fare_amount"),
            f.avg("trip_distance").alias("avg_trip_distance"),
        )
    )

    window_7d = Window.orderBy("trip_date").rowsBetween(-6, 0)

    return daily.withColumn(
        "rolling_avg_trips_7d", f.avg("total_trips").over(window_7d)
    ).orderBy("trip_date")
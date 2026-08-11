from __future__ import annotations

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

def run_raw_ingest() -> None:
    from nyc_taxi_lakehouse.spark.main_raw_ingest import main
    main()

def run_transform_silver() -> None:
    from nyc_taxi_lakehouse.spark.main_transform_silver import main
    main()

def run_validate_silver() -> None:
    from nyc_taxi_lakehouse.spark.main_validate_silver import main
    main()

def run_transform_gold() -> None:
    from nyc_taxi_lakehouse.spark.main_transform_gold import main
    main()

with DAG(
        dag_id="nyc_taxi_pipeline",
        description="Ingesta, transformación y validación de datos de taxis de NYC (raw -> bronze -> silver -> gold)",
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        schedule="@daily",
        catchup=False,
        default_args={"retries": 2},
        tags=["nyc-taxi", "lakehouse"],
) as dag:

    raw_ingest = PythonOperator(
        task_id="raw_ingest",
        python_callable=run_raw_ingest,
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_transform_silver,
    )

    validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=run_validate_silver,
    )

    transform_gold = PythonOperator(
        task_id="transform_gold",
        python_callable=run_transform_gold,
    )

    raw_ingest >> transform_silver >> validate_silver >> transform_gold














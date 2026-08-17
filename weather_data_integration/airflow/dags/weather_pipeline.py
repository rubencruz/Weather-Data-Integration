"""Phase 6 Airflow orchestration for the Weather Data Integration project.

Airflow is intentionally used as the orchestrator. The existing Databricks
Bundle remains responsible for the Spark/Delta/AI processing workload.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.empty import EmptyOperator

#JOB_ID = Variable.get("weather_databricks_job_id", default_var="0")
JOB_ID = Variable.get("WEATHER_DATABRICKS_JOB_ID", default_var=0)

DEFAULT_ARGS = {
    "owner": "weather-data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="weather_data_integration_phase6",
    description="Orchestrates the Databricks Weather Data Integration pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["weather", "databricks", "delta", "ai", "phase6"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_databricks_pipeline = DatabricksRunNowOperator(
        task_id="run_databricks_pipeline",
        databricks_conn_id="databricks_weather_data_integration",
        job_id=JOB_ID,
        wait_for_termination=True,
        polling_period_seconds=10,
    )

    end = EmptyOperator(task_id="success")

    start >> run_databricks_pipeline >> end

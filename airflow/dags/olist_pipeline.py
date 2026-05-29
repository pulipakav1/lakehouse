from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner":            "rohit",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id="olist_lakehouse_pipeline",
    default_args=default_args,
    description="Olist E-Commerce Medallion Architecture: Bronze → Silver → Gold",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["olist", "lakehouse", "medallion", "pyspark"],
)


def run_bronze_ingestion(**context):
    from bronze.bronze_ingestion import ingest_to_bronze
    results = ingest_to_bronze()
    context["ti"].xcom_push(key="bronze_results", value=str(results))
    return results


def run_validate_bronze(**context):
    from validation.bronze_validation import validate_bronze
    validate_bronze()


def run_silver_transformation(**context):
    from silver.silver_transformation import run_silver
    run_silver()


def run_validate_silver(**context):
    from validation.silver_validation import validate_silver
    validate_silver()


def run_gold_aggregation(**context):
    from gold.gold_aggregation import run_gold
    run_gold()


def run_validate_gold(**context):
    from validation.gold_validation import validate_gold
    validate_gold()


start = EmptyOperator(task_id="start", dag=dag)
end   = EmptyOperator(task_id="end",   dag=dag)

bronze_task = PythonOperator(
    task_id="bronze_ingestion",
    python_callable=run_bronze_ingestion,
    dag=dag,
)

validate_bronze_task = PythonOperator(
    task_id="validate_bronze",
    python_callable=run_validate_bronze,
    dag=dag,
)

silver_task = PythonOperator(
    task_id="silver_transformation",
    python_callable=run_silver_transformation,
    dag=dag,
)

validate_silver_task = PythonOperator(
    task_id="validate_silver",
    python_callable=run_validate_silver,
    dag=dag,
)

gold_task = PythonOperator(
    task_id="gold_aggregation",
    python_callable=run_gold_aggregation,
    dag=dag,
)

validate_gold_task = PythonOperator(
    task_id="validate_gold",
    python_callable=run_validate_gold,
    dag=dag,
)

start >> bronze_task >> validate_bronze_task >> silver_task >> validate_silver_task >> gold_task >> validate_gold_task >> end

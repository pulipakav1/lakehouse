from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# default args
default_args = {
    "owner":            "rohit",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# DAG 
dag = DAG(
    dag_id="olist_lakehouse_pipeline",
    default_args=default_args,
    description="Olist E-Commerce Medallion Architecture: Bronze → Silver → Gold",
    schedule_interval="0 6 * * *",   
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["olist", "lakehouse", "medallion", "pyspark"],
)

# task functions

def run_bronze_ingestion(**context):
    from bronze.bronze_ingestion import ingest_to_bronze
    results = ingest_to_bronze()
    context["ti"].xcom_push(key="bronze_results", value=str(results))
    return results


def run_silver_transformation(**context):
    from silver.silver_transformation import run_silver
    run_silver()


def run_gold_aggregation(**context):
    from gold.gold_aggregation import run_gold
    run_gold()


def data_quality_check(**context):
    import boto3
    import os

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    bucket = os.getenv("S3_BUCKET")
    prefix = "olist-lakehouse/bronze/"

    expected_tables = [
        "orders", "order_items", "order_payments",
        "customers", "products", "sellers", "order_reviews"
    ]

    missing = []
    for table in expected_tables:
        try:
            response = s3.list_objects_v2(
                Bucket=bucket,
                Prefix=f"{prefix}{table}/",
                MaxKeys=1
            )
            if response.get("KeyCount", 0) == 0:
                missing.append(table)
        except Exception as e:
            raise ValueError(f"S3 check failed for {table}: {e}")

    if missing:
        raise ValueError(f"Quality check failed — missing bronze tables: {missing}")

    print(f" Quality check passed — all {len(expected_tables)} bronze tables present in S3")

# tasks

start = EmptyOperator(task_id="start", dag=dag)
end   = EmptyOperator(task_id="end",   dag=dag)

bronze_task = PythonOperator(
    task_id="bronze_ingestion",
    python_callable=run_bronze_ingestion,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id="data_quality_check",
    python_callable=data_quality_check,
    dag=dag,
)

silver_task = PythonOperator(
    task_id="silver_transformation",
    python_callable=run_silver_transformation,
    dag=dag,
)

gold_task = PythonOperator(
    task_id="gold_aggregation",
    python_callable=run_gold_aggregation,
    dag=dag,
)

# dependencies
start >> bronze_task >> quality_check_task >> silver_task >> gold_task >> end

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Stack

- **PySpark 3.4** + **Delta Lake 2.4** for ingestion and transformation
- **Apache Airflow 2.8** (LocalExecutor, Postgres backend) for orchestration
- **AWS S3** (`s3a://` via `hadoop-aws`) as the storage layer for all three medallion tiers
- **Python 3.11**, **Java 17** (required by Spark), all running inside Docker

## Environment Setup

Copy `.env.example` to `.env` and fill in three required values:

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=...
# Generate fernet key:
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__CORE__FERNET_KEY=...
AIRFLOW__WEBSERVER__SECRET_KEY=...
```

## Running the Stack

```bash
# Start everything (Postgres + Airflow init/webserver/scheduler)
docker-compose up --build -d

# Airflow UI — http://localhost:8081   login: admin / admin
# Wait ~60s for init to complete before triggering

# Trigger the DAG via CLI
docker-compose exec airflow-scheduler airflow dags trigger olist_lakehouse_pipeline

# Tail scheduler logs
docker-compose logs -f airflow-scheduler

# Stop all services
docker-compose down
```

## Running Individual Pipeline Stages

Each stage can be run standalone (outside Airflow) for development. Because `PYTHONPATH=/opt/airflow` is set inside containers, run them inside the container:

```bash
docker-compose exec airflow-scheduler python /opt/airflow/bronze/bronze_ingestion.py
docker-compose exec airflow-scheduler python /opt/airflow/silver/silver_transformation.py
docker-compose exec airflow-scheduler python /opt/airflow/gold/gold_aggregation.py
```

## Architecture

### Medallion Layers

```
data/ (9 CSVs)
  → Bronze (S3 Parquet, raw + 3 metadata cols)
    → [quality gate — checks 7 tables exist in S3]
      → Silver (S3 Delta Lake, 7 cleaned tables)
        → Gold (S3 Delta Lake, 4 business aggregates)
```

### S3 Path Structure

All paths are constructed from `utils/config.py`:
- Bronze: `s3a://<bucket>/olist-lakehouse/bronze/<table_name>/`
- Silver: `s3a://<bucket>/olist-lakehouse/silver/<table_name>/`
- Gold: `s3a://<bucket>/olist-lakehouse/gold/<table_name>/`

### Module Responsibilities

| File | Role |
|---|---|
| `utils/config.py` | Central config: `create_spark_session()`, S3 path constants, `OLIST_FILES` dict, logger factory |
| `bronze/bronze_ingestion.py` | Reads 9 CSVs with `inferSchema`, adds `_ingested_at`/`_source_file`/`_ingestion_date`, writes Parquet |
| `silver/silver_transformation.py` | Per-table cleaner functions, casts types, deduplicates on natural PKs, strips bronze metadata cols, writes Delta |
| `gold/gold_aggregation.py` | Builds 4 aggregates from Silver Delta tables, writes Delta |
| `airflow/dags/olist_pipeline.py` | DAG definition; tasks call into bronze/silver/gold modules via `PythonOperator` |

### Spark Session Configuration

`create_spark_session()` in `utils/config.py` bundles Delta Lake + S3 JARs via `spark.jars.packages`. AWS credentials are read via `EnvironmentVariableCredentialsProvider` — they must be set as env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`). Shuffle partitions are capped at 4 and driver memory at 2g for laptop use.

### DAG Task Chain

```
start → bronze_ingestion → data_quality_check → silver_transformation → gold_aggregation → end
```

`data_quality_check` uses `boto3` to call `list_objects_v2` on 7 expected bronze prefixes (excludes `geolocation` and `category_translation`). It raises `ValueError` if any prefix is empty, which aborts the DAG before Silver writes.

### Silver Transformation Details

- **Orders**: casts 5 timestamp columns, derives `delivery_days` (datediff), `is_late` (bool), `order_year/month/dow`
- **Products**: left-joins `category_translation` to get English names; `category` column uses `coalesce(english, portuguese)`
- All tables: drops the 3 bronze metadata columns, deduplicates on natural key, filters null PKs

### Gold Table Logic

All four aggregates filter `order_status == "delivered"` before joining. RFM segmentation thresholds in `build_customer_segments()` are hardcoded (Champion: recency ≤ 90d, frequency ≥ 2, monetary ≥ 500). `revenue_rank` in product performance uses a `Window.partitionBy("category").orderBy(desc("total_revenue"))`.

### Docker Volume Mounts

The compose file mounts local directories directly into the container at `/opt/airflow/<dir>`, so code edits to `bronze/`, `silver/`, `gold/`, `utils/`, and `airflow/dags/` are reflected immediately without rebuilding. Rebuild is only needed when `Dockerfile.airflow` or `requirements` change.

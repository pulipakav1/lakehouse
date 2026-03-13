# Data Lakehouse Pipeline — Medallion Architecture

A production data pipeline built on the real Olist Brazilian E-Commerce dataset (100K+ orders). Implements Bronze → Silver → Gold Medallion Architecture using PySpark, Delta Lake, Apache Airflow, and AWS S3.

## Architecture

```
Olist CSVs (9 tables, 100K+ orders)
        ↓
   Bronze Layer (AWS S3)
   Raw CSVs ingested as Parquet
        ↓
   Data Quality Check
   Validates all tables present in S3
        ↓
   Silver Layer (Delta Lake on S3)
   Cleaned, typed, deduplicated — 7 tables
        ↓
   Gold Layer (Delta Lake on S3)
   4 business-ready aggregated tables
```

## Tech Stack

- **PySpark 3.4** — distributed transformations across all 3 layers
- **Delta Lake 2.4** — ACID-compliant versioned storage
- **Apache Airflow 2.8** — DAG orchestration with quality gate
- **AWS S3** — cloud storage for all layers
- **Docker Compose** — one-command local deployment

## Dataset

Olist Brazilian E-Commerce — 9 CSV files, 100K+ real orders (2016–2018)

## Gold Tables

| Table | Description |
|---|---|
| gold_sales_summary | Daily revenue, order volume, avg order value, late delivery rate |
| gold_product_performance | Revenue and ratings by product and category |
| gold_customer_segments | RFM segmentation — Champion, Loyal, New, At Risk, Lost |
| gold_seller_performance | Seller scorecard — revenue, delivery speed, avg review |

## Airflow DAG

```
start → bronze_ingestion → data_quality_check → silver_transformation → gold_aggregation → end
```

- Schedule: Daily at 6AM UTC
- Retries: 2 with 5 minute delay
- Quality gate blocks silver if bronze tables are missing




# Open Airflow UI → http://localhost:8081
# Login: admin / admin
# Trigger: olist_lakehouse_pipeline
```

## Project Structure

```
olist-lakehouse/
├── bronze/bronze_ingestion.py       ← ingest 9 CSVs to S3 as Parquet
├── silver/silver_transformation.py  ← clean, type cast, deduplicate
├── gold/gold_aggregation.py         ← build 4 business tables
├── airflow/dags/olist_pipeline.py   ← Airflow DAG
├── utils/config.py                  ← shared Spark session + S3 config
├── Dockerfile.airflow
└── docker-compose.yml
```
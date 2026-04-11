# Data Lakehouse — Medallion Architecture

<p align="center">
  <img src="https://img.shields.io/badge/PySpark-3.4-E25A1C?style=flat-square&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/Delta%20Lake-2.4-003366?style=flat-square&logo=databricks&logoColor=white"/>
  <img src="https://img.shields.io/badge/Airflow-2.8-017CEE?style=flat-square&logo=apacheairflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/AWS%20S3-FF9900?style=flat-square&logo=amazonaws&logoColor=black"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white"/>
  <img src="https://img.shields.io/badge/Orders-100K+-blue?style=flat-square"/>
</p>

> Production data pipeline over the real **Olist Brazilian
> E-Commerce dataset** (100K+ orders, 9 tables) — Bronze →
> Silver → Gold Medallion Architecture with PySpark, Delta Lake,
> Airflow quality gates, and AWS S3. Delivers RFM segments,
> seller scorecards, and sales trends in **under 11 minutes**.

---

## Architecture

```
Olist CSVs (9 tables · 100K+ orders)
            │
            ▼
┌───────────────────────────────────┐
│         BRONZE LAYER              │
│         AWS S3                    │
│  Raw CSVs ingested as Parquet     │
│  9 tables · no transformation     │
└───────────────┬───────────────────┘
                │
                ▼
┌───────────────────────────────────┐
│       DATA QUALITY GATE           │
│  Airflow task — validates all     │
│  tables present in S3             │
│  Blocks Silver if Bronze fails    │
└───────────────┬───────────────────┘
                │
                ▼
┌───────────────────────────────────┐
│         SILVER LAYER              │
│     Delta Lake on AWS S3          │
│  Cleaned · typed · deduplicated   │
│  7 tables                         │
└───────────────┬───────────────────┘
                │
                ▼
┌───────────────────────────────────┐
│          GOLD LAYER               │
│     Delta Lake on AWS S3          │
│  4 business-ready aggregated      │
│  tables for analytics             │
└───────────────────────────────────┘
```

---

## Gold Tables

| Table | Description |
|---|---|
| `gold_sales_summary` | Daily revenue · order volume · avg order value · late delivery rate |
| `gold_product_performance` | Revenue and ratings by product and category |
| `gold_customer_segments` | RFM segmentation — Champion · Loyal · New · At Risk · Lost |
| `gold_seller_performance` | Seller scorecard — revenue · delivery speed · avg review score |

---

## Airflow DAG

```
start
  │
  ▼
bronze_ingestion
  │
  ▼
data_quality_check  ← blocks pipeline if any Bronze table missing
  │
  ▼
silver_transformation
  │
  ▼
gold_aggregation
  │
  ▼
end
```

- **Schedule:** Daily at 6AM UTC
- **Retries:** 2 with 5-minute delay
- **Quality gate:** blocks Silver promotion if Bronze tables
  are missing — preventing silent data corruption downstream

---

## Stack

| Layer | Technology |
|---|---|
| Distributed Processing | PySpark 3.4 |
| Storage Format | Delta Lake 2.4 — ACID-compliant, versioned |
| Orchestration | Apache Airflow 2.8 |
| Cloud Storage | AWS S3 — all layers |
| Containerization | Docker Compose |
| Dataset | Olist Brazilian E-Commerce (2016–2018) |

---

## Why Medallion Architecture?

| Layer | Purpose |
|---|---|
| **Bronze** | Raw data preserved as-is — full audit trail, replayable |
| **Silver** | Cleaned and typed — single source of truth for analysts |
| **Gold** | Business aggregations — fast, query-ready for dashboards |

Separating concerns means a bad Silver transformation never
corrupts raw Bronze data, and Gold tables never expose messy
intermediate logic to business users.

---

## Quick Start

### Prerequisites
- Docker and Docker Compose
- AWS S3 bucket + credentials
- Olist dataset CSVs

### Setup

```bash
git clone https://github.com/pulipakav1/lakehouse.git
cd lakehouse

cp .env.example .env
# Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET in .env

docker-compose up
```

Airflow UI at **http://localhost:8080**

### Run manually (without Docker)

```bash
pip install -r requirements.txt

# Bronze ingestion
python bronze/bronze_ingestion.py

# Silver transformation
python silver/silver_transformation.py

# Gold aggregation
python gold/gold_aggregation.py
```

---

## Project Structure

```
olist-lakehouse/
├── bronze/
│   └── bronze_ingestion.py        # Ingest 9 CSVs to S3 as Parquet
├── silver/
│   └── silver_transformation.py   # Clean · type cast · deduplicate
├── gold/
│   └── gold_aggregation.py        # Build 4 business-ready tables
├── airflow/
│   └── dags/
│       └── olist_pipeline.py      # Airflow DAG definition
├── utils/
│   └── config.py                  # Shared Spark session + S3 config
├── Dockerfile.airflow
├── docker-compose.yml
└── requirements.txt
```

---

## What Makes This Different

Most data engineering projects ingest one CSV into SQLite.
This one:

- **Real dataset at scale** — 100K+ orders across 9 related
  tables, mirroring production e-commerce complexity
- **ACID storage** — Delta Lake provides versioning and
  rollback, not just flat Parquet files
- **Quality gates in the DAG** — bad data is caught at
  Bronze before it corrupts downstream layers
- **Business-ready Gold layer** — RFM segmentation and
  seller scorecards, not just raw aggregations
- **Full Docker Compose orchestration** — Airflow, Spark,
  and S3 wired together in one command

---

## License

MIT — use and modify freely.

---


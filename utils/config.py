import os
import logging
from pyspark.sql import SparkSession

S3_BUCKET  = os.getenv("S3_BUCKET", "your-bucket-name")
S3_PREFIX  = "olist-lakehouse"

BRONZE_PATH = f"s3a://{S3_BUCKET}/{S3_PREFIX}/bronze"
SILVER_PATH = f"s3a://{S3_BUCKET}/{S3_PREFIX}/silver"
GOLD_PATH   = f"s3a://{S3_BUCKET}/{S3_PREFIX}/gold"

DATA_PATH   = "/opt/airflow/data"

OLIST_FILES = {
    "customers":            "olist_customers_dataset.csv",
    "geolocation":          "olist_geolocation_dataset.csv",
    "orders":               "olist_orders_dataset.csv",
    "order_items":          "olist_order_items_dataset.csv",
    "order_payments":       "olist_order_payments_dataset.csv",
    "order_reviews":        "olist_order_reviews_dataset.csv",
    "products":             "olist_products_dataset.csv",
    "sellers":              "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

RFM_CHAMPION_RECENCY   = 90
RFM_CHAMPION_FREQUENCY = 2
RFM_CHAMPION_MONETARY  = 500
RFM_LOYAL_RECENCY      = 180
RFM_LOYAL_FREQUENCY    = 2
RFM_NEW_RECENCY        = 90
RFM_LOST_RECENCY       = 365


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    )
    return logging.getLogger(name)


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

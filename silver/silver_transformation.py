import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, TimestampType, StringType
)
from utils.config import (
    create_spark_session, get_logger,
    BRONZE_PATH, SILVER_PATH
)

logger = get_logger("silver")


# table cleaners

def clean_orders(spark):
    logger.info("Cleaning orders table")
    df = spark.read.parquet(f"{BRONZE_PATH}/orders")

    df = (
        df
        # Cast timestamps
        .withColumn("order_purchase_timestamp",
                    F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at",
                    F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date",
                    F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date",
                    F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date",
                    F.to_timestamp("order_estimated_delivery_date"))
        # Add derived columns
        .withColumn("order_year",  F.year("order_purchase_timestamp"))
        .withColumn("order_month", F.month("order_purchase_timestamp"))
        .withColumn("order_dow",   F.dayofweek("order_purchase_timestamp"))
        .withColumn("delivery_days",
                    F.datediff(
                        F.col("order_delivered_customer_date"),
                        F.col("order_purchase_timestamp")
                    ))
        .withColumn("is_late",
                    F.col("order_delivered_customer_date") >
                    F.col("order_estimated_delivery_date"))
        # Drop metadata columns
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        # Remove duplicates
        .dropDuplicates(["order_id"])
        # Drop nulls on key column
        .filter(F.col("order_id").isNotNull())
    )

    count = df.count()
    logger.info(f"Orders silver: {count:,} rows")
    return df


def clean_order_items(spark):
    logger.info("Cleaning order_items table")
    df = spark.read.parquet(f"{BRONZE_PATH}/order_items")

    df = (
        df
        .withColumn("price",              F.col("price").cast(DoubleType()))
        .withColumn("freight_value",      F.col("freight_value").cast(DoubleType()))
        .withColumn("order_item_id",      F.col("order_item_id").cast(IntegerType()))
        .withColumn("shipping_limit_date",F.to_timestamp("shipping_limit_date"))
        .withColumn("total_item_value",   F.col("price") + F.col("freight_value"))
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["order_id", "order_item_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("price") > 0)
    )

    count = df.count()
    logger.info(f"Order items silver: {count:,} rows")
    return df


def clean_order_payments(spark):
    logger.info("Cleaning order_payments table")
    df = spark.read.parquet(f"{BRONZE_PATH}/order_payments")

    df = (
        df
        .withColumn("payment_value",        F.col("payment_value").cast(DoubleType()))
        .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
        .withColumn("payment_sequential",   F.col("payment_sequential").cast(IntegerType()))
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("payment_value") > 0)
    )

    count = df.count()
    logger.info(f"Order payments silver: {count:,} rows")
    return df


def clean_customers(spark):
    logger.info("Cleaning customers table")
    df = spark.read.parquet(f"{BRONZE_PATH}/customers")

    df = (
        df
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["customer_id"])
        .filter(F.col("customer_id").isNotNull())
        .withColumnRenamed("customer_city",  "customer_city")
        .withColumnRenamed("customer_state", "customer_state")
    )

    count = df.count()
    logger.info(f"Customers silver: {count:,} rows")
    return df


def clean_products(spark):
    logger.info("Cleaning products table")
    df_products = spark.read.parquet(f"{BRONZE_PATH}/products")
    df_translate = spark.read.parquet(f"{BRONZE_PATH}/category_translation")

    df = (
        df_products
        .join(df_translate, on="product_category_name", how="left")
        .withColumn("product_name_lenght",
                    F.col("product_name_lenght").cast(IntegerType()))
        .withColumn("product_description_lenght",
                    F.col("product_description_lenght").cast(IntegerType()))
        .withColumn("product_photos_qty",
                    F.col("product_photos_qty").cast(IntegerType()))
        .withColumn("product_weight_g",
                    F.col("product_weight_g").cast(DoubleType()))
        .withColumn("product_length_cm",
                    F.col("product_length_cm").cast(DoubleType()))
        .withColumn("product_height_cm",
                    F.col("product_height_cm").cast(DoubleType()))
        .withColumn("product_width_cm",
                    F.col("product_width_cm").cast(DoubleType()))
        # Use English category name, fallback to Portuguese
        .withColumn("category",
                    F.coalesce(
                        F.col("product_category_name_english"),
                        F.col("product_category_name")
                    ))
        .drop("_ingested_at", "_source_file", "_ingestion_date",
              "product_category_name", "product_category_name_english")
        .dropDuplicates(["product_id"])
        .filter(F.col("product_id").isNotNull())
    )

    count = df.count()
    logger.info(f"Products silver: {count:,} rows")
    return df


def clean_sellers(spark):
    logger.info("Cleaning sellers table")
    df = spark.read.parquet(f"{BRONZE_PATH}/sellers")

    df = (
        df
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["seller_id"])
        .filter(F.col("seller_id").isNotNull())
    )

    count = df.count()
    logger.info(f"Sellers silver: {count:,} rows")
    return df


def clean_order_reviews(spark):
    logger.info("Cleaning order_reviews table")
    df = spark.read.parquet(f"{BRONZE_PATH}/order_reviews")

    df = (
        df
        .withColumn("review_score",
                    F.col("review_score").cast(IntegerType()))
        .withColumn("review_creation_date",
                    F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp",
                    F.to_timestamp("review_answer_timestamp"))
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["review_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("review_score").isNotNull())
    )

    count = df.count()
    logger.info(f"Order reviews silver: {count:,} rows")
    return df


# silver

def write_silver(df, table_name: str):
    output_path = f"{SILVER_PATH}/{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(output_path)
    )
    logger.info(f" Written to Silver: {output_path}")


# main

def run_silver():
    logger.info("Starting Silver transformation layer")
    spark = create_spark_session("OlistSilverTransformation")
    spark.sparkContext.setLogLevel("WARN")

    tables = {
        "orders":         clean_orders(spark),
        "order_items":    clean_order_items(spark),
        "order_payments": clean_order_payments(spark),
        "customers":      clean_customers(spark),
        "products":       clean_products(spark),
        "sellers":        clean_sellers(spark),
        "order_reviews":  clean_order_reviews(spark),
    }

    for table_name, df in tables.items():
        write_silver(df, table_name)

    spark.stop()
    logger.info("Silver layer complete — 7 tables written as Delta Lake")


if __name__ == "__main__":
    run_silver()

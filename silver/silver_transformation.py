import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from delta.tables import DeltaTable
from utils.config import (
    create_spark_session, get_logger,
    BRONZE_PATH, SILVER_PATH
)

logger = get_logger("silver")


def _transform_orders(df):
    return (
        df
        .withColumn("order_purchase_timestamp",      F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at",             F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date",  F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
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
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull())
    )


def _transform_order_items(df):
    return (
        df
        .withColumn("price",               F.col("price").cast(DoubleType()))
        .withColumn("freight_value",       F.col("freight_value").cast(DoubleType()))
        .withColumn("order_item_id",       F.col("order_item_id").cast(IntegerType()))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
        .withColumn("total_item_value",    F.col("price") + F.col("freight_value"))
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["order_id", "order_item_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("price") > 0)
    )


def _transform_order_payments(df):
    return (
        df
        .withColumn("payment_value",        F.col("payment_value").cast(DoubleType()))
        .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
        .withColumn("payment_sequential",   F.col("payment_sequential").cast(IntegerType()))
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["order_id", "payment_sequential"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("payment_value") > 0)
    )


def _transform_customers(df):
    return (
        df
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["customer_id"])
        .filter(F.col("customer_id").isNotNull())
    )


def _transform_products(df_products, df_translate):
    return (
        df_products
        .join(df_translate, on="product_category_name", how="left")
        .withColumn("product_name_lenght",        F.col("product_name_lenght").cast(IntegerType()))
        .withColumn("product_description_lenght", F.col("product_description_lenght").cast(IntegerType()))
        .withColumn("product_photos_qty",         F.col("product_photos_qty").cast(IntegerType()))
        .withColumn("product_weight_g",           F.col("product_weight_g").cast(DoubleType()))
        .withColumn("product_length_cm",          F.col("product_length_cm").cast(DoubleType()))
        .withColumn("product_height_cm",          F.col("product_height_cm").cast(DoubleType()))
        .withColumn("product_width_cm",           F.col("product_width_cm").cast(DoubleType()))
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


def _transform_sellers(df):
    return (
        df
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["seller_id"])
        .filter(F.col("seller_id").isNotNull())
    )


def _transform_order_reviews(df):
    return (
        df
        .withColumn("review_score",            F.col("review_score").cast(IntegerType()))
        .withColumn("review_creation_date",    F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["review_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("review_score").isNotNull())
    )


def _transform_geolocation(df):
    return (
        df
        .withColumn("geolocation_lat", F.col("geolocation_lat").cast(DoubleType()))
        .withColumn("geolocation_lng", F.col("geolocation_lng").cast(DoubleType()))
        .drop("_ingested_at", "_source_file", "_ingestion_date")
        .dropDuplicates(["geolocation_zip_code_prefix"])
        .filter(F.col("geolocation_zip_code_prefix").isNotNull())
    )


def clean_orders(spark):
    df = spark.read.parquet(f"{BRONZE_PATH}/orders")
    result = _transform_orders(df)
    logger.info(f"Orders silver: {result.count():,} rows")
    return result


def clean_order_items(spark):
    df = spark.read.parquet(f"{BRONZE_PATH}/order_items")
    result = _transform_order_items(df)
    logger.info(f"Order items silver: {result.count():,} rows")
    return result


def clean_order_payments(spark):
    df = spark.read.parquet(f"{BRONZE_PATH}/order_payments")
    result = _transform_order_payments(df)
    logger.info(f"Order payments silver: {result.count():,} rows")
    return result


def clean_customers(spark):
    df = spark.read.parquet(f"{BRONZE_PATH}/customers")
    result = _transform_customers(df)
    logger.info(f"Customers silver: {result.count():,} rows")
    return result


def clean_products(spark):
    df_products  = spark.read.parquet(f"{BRONZE_PATH}/products")
    df_translate = spark.read.parquet(f"{BRONZE_PATH}/category_translation")
    result = _transform_products(df_products, df_translate)
    logger.info(f"Products silver: {result.count():,} rows")
    return result


def clean_sellers(spark):
    df = spark.read.parquet(f"{BRONZE_PATH}/sellers")
    result = _transform_sellers(df)
    logger.info(f"Sellers silver: {result.count():,} rows")
    return result


def clean_order_reviews(spark):
    df = spark.read.parquet(f"{BRONZE_PATH}/order_reviews")
    result = _transform_order_reviews(df)
    logger.info(f"Order reviews silver: {result.count():,} rows")
    return result


def clean_geolocation(spark):
    df = spark.read.parquet(f"{BRONZE_PATH}/geolocation")
    result = _transform_geolocation(df)
    logger.info(f"Geolocation silver: {result.count():,} rows")
    return result


def write_silver_merge(spark, df, table_name, merge_keys):
    output_path = f"{SILVER_PATH}/{table_name}"
    merge_condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    if DeltaTable.isDeltaTable(spark, output_path):
        (
            DeltaTable.forPath(spark, output_path).alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(output_path)
    logger.info(f"Written to Silver: {output_path}")


def run_silver():
    logger.info("Starting Silver transformation layer")
    spark = create_spark_session("OlistSilverTransformation")
    spark.sparkContext.setLogLevel("WARN")

    tables = {
        "orders":         (clean_orders(spark),         ["order_id"]),
        "order_items":    (clean_order_items(spark),    ["order_id", "order_item_id"]),
        "order_payments": (clean_order_payments(spark), ["order_id", "payment_sequential"]),
        "customers":      (clean_customers(spark),      ["customer_id"]),
        "products":       (clean_products(spark),       ["product_id"]),
        "sellers":        (clean_sellers(spark),        ["seller_id"]),
        "order_reviews":  (clean_order_reviews(spark),  ["review_id"]),
        "geolocation":    (clean_geolocation(spark),    ["geolocation_zip_code_prefix"]),
    }

    for table_name, (df, merge_keys) in tables.items():
        write_silver_merge(spark, df, table_name, merge_keys)

    spark.stop()
    logger.info("Silver layer complete — 8 tables written as Delta Lake")


if __name__ == "__main__":
    run_silver()

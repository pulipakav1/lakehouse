import sys
sys.path.insert(0, "/opt/airflow")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.config import (
    create_spark_session, get_logger,
    SILVER_PATH, GOLD_PATH
)

logger = get_logger("gold")


def build_sales_summary(spark):
    """Daily and monthly revenue, order volume, avg order value."""
    logger.info("Building gold_sales_summary")

    orders   = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    payments = spark.read.format("delta").load(f"{SILVER_PATH}/order_payments")
    items    = spark.read.format("delta").load(f"{SILVER_PATH}/order_items")

    # Join orders + payments
    order_payments = (
        payments
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment"),
            F.countDistinct("payment_type").alias("payment_methods_used")
        )
    )

    order_items_agg = (
        items
        .groupBy("order_id")
        .agg(
            F.sum("price").alias("items_revenue"),
            F.sum("freight_value").alias("freight_revenue"),
            F.count("order_item_id").alias("items_count")
        )
    )

    df = (
        orders
        .filter(F.col("order_status") == "delivered")
        .join(order_payments,   on="order_id", how="left")
        .join(order_items_agg,  on="order_id", how="left")
        .groupBy("order_year", "order_month",
                 F.to_date("order_purchase_timestamp").alias("order_date"))
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_payment").alias("total_revenue"),
            F.avg("total_payment").alias("avg_order_value"),
            F.sum("items_count").alias("total_items_sold"),
            F.sum("freight_revenue").alias("total_freight"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.sum(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_deliveries")
        )
        .withColumn("late_delivery_rate",
                    F.round(F.col("late_deliveries") / F.col("total_orders") * 100, 2))
        .withColumn("revenue_per_item",
                    F.round(F.col("total_revenue") / F.col("total_items_sold"), 2))
        .orderBy("order_date")
    )

    count = df.count()
    logger.info(f"Sales summary: {count:,} rows")
    return df


def build_product_performance(spark):
    """Revenue, volume, avg rating per product and category."""
    logger.info("Building gold_product_performance")

    items    = spark.read.format("delta").load(f"{SILVER_PATH}/order_items")
    products = spark.read.format("delta").load(f"{SILVER_PATH}/products")
    orders   = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    reviews  = spark.read.format("delta").load(f"{SILVER_PATH}/order_reviews")

    # Delivered orders only
    delivered = orders.filter(F.col("order_status") == "delivered").select("order_id")

    avg_reviews = (
        reviews
        .groupBy("order_id")
        .agg(F.avg("review_score").alias("avg_review_score"))
    )

    df = (
        items
        .join(delivered,    on="order_id",   how="inner")
        .join(products,     on="product_id", how="left")
        .join(avg_reviews,  on="order_id",   how="left")
        .groupBy("product_id", "category")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("price").alias("total_revenue"),
            F.avg("price").alias("avg_price"),
            F.sum("freight_value").alias("total_freight"),
            F.avg("avg_review_score").alias("avg_rating"),
            F.countDistinct("order_id").alias("unique_orders")
        )
        .withColumn("revenue_rank",
                    F.rank().over(
                        Window.partitionBy("category")
                        .orderBy(F.desc("total_revenue"))
                    ))
        .withColumn("freight_ratio",
                    F.round(F.col("total_freight") / F.col("total_revenue") * 100, 2))
        .orderBy(F.desc("total_revenue"))
    )

    count = df.count()
    logger.info(f"Product performance: {count:,} rows")
    return df


def build_customer_segments(spark):
    """RFM-style customer segmentation — Recency, Frequency, Monetary."""
    logger.info("Building gold_customer_segments")

    orders   = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    payments = spark.read.format("delta").load(f"{SILVER_PATH}/order_payments")
    customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")

    max_date = orders.agg(F.max("order_purchase_timestamp")).collect()[0][0]

    order_payments = (
        payments
        .groupBy("order_id")
        .agg(F.sum("payment_value").alias("order_value"))
    )

    rfm = (
        orders
        .filter(F.col("order_status") == "delivered")
        .join(order_payments, on="order_id", how="left")
        .groupBy("customer_id")
        .agg(
            F.datediff(F.lit(max_date),
                       F.max("order_purchase_timestamp")).alias("recency_days"),
            F.count("order_id").alias("frequency"),
            F.sum("order_value").alias("monetary")
        )
    )

    # Simple segmentation based on thresholds
    df = (
        rfm
        .join(customers, on="customer_id", how="left")
        .withColumn("segment",
            F.when(
                (F.col("recency_days") <= 90) &
                (F.col("frequency") >= 2) &
                (F.col("monetary") >= 500), "Champion"
            ).when(
                (F.col("recency_days") <= 180) &
                (F.col("frequency") >= 2), "Loyal"
            ).when(
                (F.col("recency_days") <= 90) &
                (F.col("frequency") == 1), "New Customer"
            ).when(
                F.col("recency_days") > 365, "Lost"
            ).otherwise("At Risk")
        )
        .withColumn("monetary", F.round("monetary", 2))
        .select("customer_id", "customer_state", "customer_city",
                "recency_days", "frequency", "monetary", "segment")
    )

    count = df.count()
    logger.info(f"Customer segments: {count:,} rows")
    return df


def build_seller_performance(spark):
    """Seller scorecard — revenue, delivery speed, ratings."""
    logger.info("Building gold_seller_performance")

    items   = spark.read.format("delta").load(f"{SILVER_PATH}/order_items")
    orders  = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
    sellers = spark.read.format("delta").load(f"{SILVER_PATH}/sellers")
    reviews = spark.read.format("delta").load(f"{SILVER_PATH}/order_reviews")

    avg_reviews = (
        reviews
        .groupBy("order_id")
        .agg(F.avg("review_score").alias("avg_score"))
    )

    df = (
        items
        .join(orders.filter(F.col("order_status") == "delivered"),
              on="order_id", how="inner")
        .join(avg_reviews, on="order_id", how="left")
        .groupBy("seller_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("price").alias("total_revenue"),
            F.avg("price").alias("avg_item_price"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.avg("avg_score").alias("avg_review_score"),
            F.sum(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_orders"),
            F.countDistinct("product_id").alias("unique_products")
        )
        .join(sellers, on="seller_id", how="left")
        .withColumn("late_rate",
                    F.round(F.col("late_orders") / F.col("total_orders") * 100, 2))
        .withColumn("total_revenue", F.round("total_revenue", 2))
        .withColumn("avg_review_score", F.round("avg_review_score", 2))
        .orderBy(F.desc("total_revenue"))
    )

    count = df.count()
    logger.info(f"Seller performance: {count:,} rows")
    return df


# gold

def write_gold(df, table_name: str):
    output_path = f"{GOLD_PATH}/{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(output_path)
    )
    logger.info(f"✓ Written to Gold: {output_path}")


# main
def run_gold():
    logger.info("Starting Gold aggregation layer")
    spark = create_spark_session("OlistGoldAggregation")
    spark.sparkContext.setLogLevel("WARN")

    tables = {
        "gold_sales_summary":       build_sales_summary(spark),
        "gold_product_performance": build_product_performance(spark),
        "gold_customer_segments":   build_customer_segments(spark),
        "gold_seller_performance":  build_seller_performance(spark),
    }

    for table_name, df in tables.items():
        write_gold(df, table_name)

    spark.stop()
    logger.info("Gold layer complete — 4 business-ready tables written")


if __name__ == "__main__":
    run_gold()

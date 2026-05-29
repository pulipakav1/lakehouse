import sys
sys.path.insert(0, "/opt/airflow")

from utils.config import create_spark_session, get_logger, GOLD_PATH
from validation.gx_utils import run_suite

logger = get_logger("validate_gold")

_CUSTOMER_SEGMENTS = ["Champion", "Loyal", "New Customer", "At Risk", "Lost"]


def _sales_summary_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("order_date"),
        gdf.expect_column_values_to_be_between("total_orders", 1, None),
        gdf.expect_column_values_to_be_between("total_revenue", 0.01, None),
        gdf.expect_column_values_to_be_between("late_delivery_rate", 0, 100),
    ]


def _product_performance_suite(gdf):
    return [
        gdf.expect_column_values_to_be_between("revenue_rank", 1, None),
        gdf.expect_column_values_to_be_between("freight_ratio", 0, None),
        gdf.expect_column_values_to_be_between("total_revenue", 0.01, None),
    ]


def _customer_segments_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("customer_id"),
        gdf.expect_column_values_to_be_in_set("segment", _CUSTOMER_SEGMENTS),
    ]


def _seller_performance_suite(gdf):
    return [
        gdf.expect_column_values_to_be_between("total_orders", 1, None),
        gdf.expect_column_values_to_be_between("late_rate", 0, 100),
        gdf.expect_column_values_to_be_between("avg_review_score", 1, 5),
    ]


_GOLD_SUITES = {
    "gold_sales_summary":       _sales_summary_suite,
    "gold_product_performance": _product_performance_suite,
    "gold_customer_segments":   _customer_segments_suite,
    "gold_seller_performance":  _seller_performance_suite,
}


def validate_gold():
    logger.info("Starting Gold validation — 4 tables")
    spark = create_spark_session("OlistValidateGold")
    spark.sparkContext.setLogLevel("WARN")

    for table_name, suite_fn in _GOLD_SUITES.items():
        logger.info(f"Validating gold/{table_name}")
        df = spark.read.format("delta").load(f"{GOLD_PATH}/{table_name}")
        run_suite(df, suite_fn, table_name)

    spark.stop()
    logger.info("Gold validation complete — all 4 tables passed")


if __name__ == "__main__":
    validate_gold()

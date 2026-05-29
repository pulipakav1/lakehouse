import sys
sys.path.insert(0, "/opt/airflow")

from utils.config import create_spark_session, get_logger, BRONZE_PATH
from validation.gx_utils import run_suite

logger = get_logger("validate_bronze")

_ORDER_STATUSES = [
    "delivered", "shipped", "canceled", "unavailable",
    "invoiced", "processing", "created", "approved",
]

_PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]


def _orders_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("order_id"),
        gdf.expect_column_values_to_be_unique("order_id"),
        gdf.expect_table_row_count_to_be_between(min_value=90_000, max_value=120_000),
        gdf.expect_column_values_to_be_in_set("order_status", _ORDER_STATUSES),
        gdf.expect_column_values_to_not_be_null("order_purchase_timestamp"),
    ]


def _order_items_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("order_id"),
        gdf.expect_column_values_to_not_be_null("product_id"),
        gdf.expect_column_values_to_be_between("price", 0, 10_000),
        gdf.expect_column_values_to_be_between("freight_value", 0, 1_000),
    ]


def _order_payments_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("order_id"),
        gdf.expect_column_values_to_be_in_set("payment_type", _PAYMENT_TYPES),
        gdf.expect_column_values_to_be_between("payment_value", 0, 100_000),
    ]


def _customers_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("customer_id"),
        gdf.expect_column_values_to_be_unique("customer_id"),
        gdf.expect_column_values_to_not_be_null("customer_state"),
    ]


def _products_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("product_id"),
        gdf.expect_column_values_to_be_unique("product_id"),
    ]


def _sellers_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("seller_id"),
        gdf.expect_column_values_to_be_unique("seller_id"),
    ]


def _order_reviews_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("review_id", mostly=0.999),
        gdf.expect_column_values_to_be_between("review_score", 1, 5, mostly=0.97),
        gdf.expect_column_values_to_not_be_null("order_id", mostly=0.97),
    ]


def _geolocation_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("geolocation_zip_code_prefix"),
        gdf.expect_column_values_to_be_between("geolocation_lat", -34, 6),
        gdf.expect_column_values_to_be_between("geolocation_lng", -74, -34),
    ]


_BRONZE_SUITES = {
    "orders":         _orders_suite,
    "order_items":    _order_items_suite,
    "order_payments": _order_payments_suite,
    "customers":      _customers_suite,
    "products":       _products_suite,
    "sellers":        _sellers_suite,
    "order_reviews":  _order_reviews_suite,
    "geolocation":    _geolocation_suite,
}


def validate_bronze():
    logger.info("Starting Bronze validation — 8 tables")
    spark = create_spark_session("OlistValidateBronze")
    spark.sparkContext.setLogLevel("WARN")

    for table_name, suite_fn in _BRONZE_SUITES.items():
        logger.info(f"Validating bronze/{table_name}")
        df = spark.read.parquet(f"{BRONZE_PATH}/{table_name}")
        run_suite(df, suite_fn, table_name)

    spark.stop()
    logger.info("Bronze validation complete — all 8 tables passed")


if __name__ == "__main__":
    validate_bronze()

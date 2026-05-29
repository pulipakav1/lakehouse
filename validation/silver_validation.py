import sys
sys.path.insert(0, "/opt/airflow")

from utils.config import create_spark_session, get_logger, SILVER_PATH
from validation.gx_utils import run_suite

logger = get_logger("validate_silver")

_BRONZE_METADATA_COLS = ["_ingested_at", "_source_file", "_ingestion_date"]
_ORDERS_DERIVED_COLS = ["is_late", "delivery_days", "order_year", "order_month", "order_dow"]


def _check_no_metadata_cols(df, table_name):
    leftover = [c for c in _BRONZE_METADATA_COLS if c in df.columns]
    if leftover:
        raise ValueError(
            f"Silver '{table_name}' still contains bronze metadata cols: {leftover}"
        )
    logger.info(f"  [PASS] No bronze metadata cols in '{table_name}'")


def _orders_suite(gdf):
    results = [
        gdf.expect_column_values_to_not_be_null("order_id"),
        gdf.expect_column_values_to_be_unique("order_id"),
    ]
    for col in _ORDERS_DERIVED_COLS:
        results.append(gdf.expect_column_to_exist(col))
    results.append(gdf.expect_column_values_to_be_between("order_year", 2016, 2018))
    return results


def _order_items_suite(gdf):
    return [
        gdf.expect_column_to_exist("total_item_value"),
        gdf.expect_column_values_to_be_between("price", 0.01, 10_000),
    ]


def _order_payments_suite(gdf):
    return [
        gdf.expect_column_values_to_be_between("payment_value", 0.01, 100_000),
    ]


def _customers_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("customer_id"),
        gdf.expect_column_values_to_be_unique("customer_id"),
    ]


def _products_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("product_id"),
        gdf.expect_column_to_exist("category"),
    ]


def _sellers_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("seller_id"),
    ]


def _order_reviews_suite(gdf):
    return [
        gdf.expect_column_values_to_be_between("review_score", 1, 5),
    ]


def _geolocation_suite(gdf):
    return [
        gdf.expect_column_values_to_not_be_null("geolocation_zip_code_prefix"),
        gdf.expect_column_values_to_be_between("geolocation_lat", -34, 6),
        gdf.expect_column_values_to_be_between("geolocation_lng", -74, -34),
    ]


_SILVER_SUITES = {
    "orders":         _orders_suite,
    "order_items":    _order_items_suite,
    "order_payments": _order_payments_suite,
    "customers":      _customers_suite,
    "products":       _products_suite,
    "sellers":        _sellers_suite,
    "order_reviews":  _order_reviews_suite,
    "geolocation":    _geolocation_suite,
}


def validate_silver():
    logger.info("Starting Silver validation — 8 tables")
    spark = create_spark_session("OlistValidateSilver")
    spark.sparkContext.setLogLevel("WARN")

    for table_name, suite_fn in _SILVER_SUITES.items():
        logger.info(f"Validating silver/{table_name}")
        df = spark.read.format("delta").load(f"{SILVER_PATH}/{table_name}")
        _check_no_metadata_cols(df, table_name)
        run_suite(df, suite_fn, table_name)

    spark.stop()
    logger.info("Silver validation complete — all 8 tables passed")


if __name__ == "__main__":
    validate_silver()

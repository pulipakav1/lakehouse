import sys
sys.path.insert(0, "/opt/airflow")

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)
from silver.silver_transformation import (
    _transform_orders,
    _transform_products,
    _transform_geolocation,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_silver")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


_ORDERS_SCHEMA = StructType([
    StructField("order_id",                       StringType(), True),
    StructField("order_status",                   StringType(), True),
    StructField("customer_id",                    StringType(), True),
    StructField("order_purchase_timestamp",       StringType(), True),
    StructField("order_approved_at",              StringType(), True),
    StructField("order_delivered_carrier_date",   StringType(), True),
    StructField("order_delivered_customer_date",  StringType(), True),
    StructField("order_estimated_delivery_date",  StringType(), True),
    StructField("_ingested_at",                   StringType(), True),
    StructField("_source_file",                   StringType(), True),
    StructField("_ingestion_date",                StringType(), True),
])

_PRODUCTS_SCHEMA = StructType([
    StructField("product_id",                    StringType(),  True),
    StructField("product_category_name",         StringType(),  True),
    StructField("product_name_lenght",           IntegerType(), True),
    StructField("product_description_lenght",    IntegerType(), True),
    StructField("product_photos_qty",            IntegerType(), True),
    StructField("product_weight_g",              DoubleType(),  True),
    StructField("product_length_cm",             DoubleType(),  True),
    StructField("product_height_cm",             DoubleType(),  True),
    StructField("product_width_cm",              DoubleType(),  True),
    StructField("_ingested_at",                  StringType(),  True),
    StructField("_source_file",                  StringType(),  True),
    StructField("_ingestion_date",               StringType(),  True),
])

_TRANSLATE_SCHEMA = StructType([
    StructField("product_category_name",         StringType(), True),
    StructField("product_category_name_english", StringType(), True),
])


def test_orders_delivery_days(spark):
    data = [("o1", "delivered", "c1", "2018-01-01 00:00:00", None, None,
             "2018-01-10 00:00:00", "2018-01-15 00:00:00", "ts", "f", "2026-01-01")]
    result = _transform_orders(spark.createDataFrame(data, _ORDERS_SCHEMA)).collect()
    assert result[0].delivery_days == 9


def test_orders_is_late(spark):
    data = [
        ("o1", "d", "c1", "2018-01-01 00:00:00", None, None,
         "2018-01-10 00:00:00", "2018-01-08 00:00:00", "ts", "f", "2026-01-01"),
        ("o2", "d", "c2", "2018-01-01 00:00:00", None, None,
         "2018-01-05 00:00:00", "2018-01-10 00:00:00", "ts", "f", "2026-01-01"),
    ]
    rows = {r.order_id: r for r in _transform_orders(spark.createDataFrame(data, _ORDERS_SCHEMA)).collect()}
    assert rows["o1"].is_late is True
    assert rows["o2"].is_late is False


def test_orders_dedup_and_null_filter(spark):
    data = [
        ("o1", "d", "c1", "2018-01-01 00:00:00", None, None, None, None, "ts", "f", "2026-01-01"),
        ("o1", "d", "c1", "2018-01-01 00:00:00", None, None, None, None, "ts", "f", "2026-01-01"),
        (None, "d", "c2", "2018-01-01 00:00:00", None, None, None, None, "ts", "f", "2026-01-01"),
    ]
    result = _transform_orders(spark.createDataFrame(data, _ORDERS_SCHEMA)).collect()
    assert len(result) == 1
    assert result[0].order_id == "o1"


def test_orders_metadata_cols_dropped(spark):
    data = [("o1", "d", "c1", "2018-01-01 00:00:00", None, None, None, None, "ts", "f", "2026-01-01")]
    result_cols = _transform_orders(spark.createDataFrame(data, _ORDERS_SCHEMA)).columns
    assert "_ingested_at" not in result_cols
    assert "_source_file" not in result_cols
    assert "_ingestion_date" not in result_cols


def test_products_category_coalesce(spark):
    products = spark.createDataFrame([
        ("p1", "esporte_lazer", None, None, None, None, None, None, None, "ts", "f", "2026-01-01"),
        ("p2", "unknown_cat",   None, None, None, None, None, None, None, "ts", "f", "2026-01-01"),
    ], _PRODUCTS_SCHEMA)
    translate = spark.createDataFrame([
        ("esporte_lazer", "sports_leisure"),
    ], _TRANSLATE_SCHEMA)
    rows = {r.product_id: r for r in _transform_products(products, translate).collect()}
    assert rows["p1"].category == "sports_leisure"
    assert rows["p2"].category == "unknown_cat"


def test_geolocation_dedup(spark):
    data = [
        (10000, -23.5, -46.6, "sao paulo",      "SP", "ts", "f", "2026-01-01"),
        (10000, -23.6, -46.7, "sao paulo",      "SP", "ts", "f", "2026-01-01"),
        (20000, -22.9, -43.1, "rio de janeiro", "RJ", "ts", "f", "2026-01-01"),
    ]
    cols = [
        "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
        "geolocation_city", "geolocation_state",
        "_ingested_at", "_source_file", "_ingestion_date",
    ]
    result = _transform_geolocation(spark.createDataFrame(data, cols)).collect()
    assert len(result) == 2
    zips = {r.geolocation_zip_code_prefix for r in result}
    assert 10000 in zips
    assert 20000 in zips

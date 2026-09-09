# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: immutable-shape Olist ingestion
# MAGIC Reads every source CSV with an explicit all-string schema, adds file lineage, and replaces the managed Bronze snapshot transactionally.

# COMMAND ----------

import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def safe_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def string_schema(columns: list[str]) -> StructType:
    return StructType([StructField(column, StringType(), True) for column in columns])


current_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]
dbutils.widgets.text("catalog", current_catalog, "Unity Catalog catalog")
dbutils.widgets.text("base_schema", "ecommerce", "Base schema name")
catalog = safe_identifier(dbutils.widgets.get("catalog"))
base_schema = safe_identifier(dbutils.widgets.get("base_schema"))
bronze_schema = f"{base_schema}_bronze"
source_path = f"/Volumes/{catalog}/{bronze_schema}/olist_raw"

TABLE_COLUMNS = {
    "customers": ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"],
    "order_items": ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"],
    "order_payments": ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
    "order_reviews": ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"],
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"],
    "products": ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"],
    "sellers": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    "product_category_name_translation": ["product_category_name", "product_category_name_english"],
}

for table_name, columns in TABLE_COLUMNS.items():
    file_name = f"olist_{table_name}_dataset.csv" if table_name != "product_category_name_translation" else f"{table_name}.csv"
    frame = (
        spark.read
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(string_schema(columns))
        .csv(f"{source_path}/{file_name}")
        # Unity Catalog file sources expose lineage through the hidden metadata struct.
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    target = f"`{catalog}`.`{bronze_schema}`.`olist_{table_name}`"
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"{target}: {frame.count():,} rows")

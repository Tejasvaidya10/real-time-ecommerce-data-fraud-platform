# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: typed, deduplicated business tables
# MAGIC Preserves Olist business keys, standardizes types, translates product categories, and exposes one fact or dimension at a declared grain.

# COMMAND ----------

import re


def safe_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


current_catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]
dbutils.widgets.text("catalog", current_catalog, "Unity Catalog catalog")
dbutils.widgets.text("base_schema", "ecommerce", "Base schema name")
catalog = safe_identifier(dbutils.widgets.get("catalog"))
base_schema = safe_identifier(dbutils.widgets.get("base_schema"))
b = f"`{catalog}`.`{base_schema}_bronze`"
s = f"`{catalog}`.`{base_schema}_silver`"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {s}.dim_customers AS
SELECT DISTINCT customer_id, customer_unique_id,
       CAST(customer_zip_code_prefix AS INT) AS zip_code_prefix,
       lower(trim(customer_city)) AS city, upper(trim(customer_state)) AS state
FROM {b}.olist_customers
WHERE customer_id IS NOT NULL AND customer_unique_id IS NOT NULL
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {s}.dim_sellers AS
SELECT DISTINCT seller_id, CAST(seller_zip_code_prefix AS INT) AS zip_code_prefix,
       lower(trim(seller_city)) AS city, upper(trim(seller_state)) AS state
FROM {b}.olist_sellers WHERE seller_id IS NOT NULL
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {s}.dim_products AS
SELECT DISTINCT p.product_id,
       coalesce(t.product_category_name_english, p.product_category_name, 'unknown') AS category,
       CAST(p.product_name_lenght AS INT) AS name_length,
       CAST(p.product_description_lenght AS INT) AS description_length,
       CAST(p.product_photos_qty AS INT) AS photo_count,
       CAST(p.product_weight_g AS DOUBLE) AS weight_g,
       CAST(p.product_length_cm AS DOUBLE) AS length_cm,
       CAST(p.product_height_cm AS DOUBLE) AS height_cm,
       CAST(p.product_width_cm AS DOUBLE) AS width_cm
FROM {b}.olist_products p
LEFT JOIN {b}.olist_product_category_name_translation t USING (product_category_name)
WHERE p.product_id IS NOT NULL
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {s}.dim_geolocation_zip AS
WITH candidates AS (
  SELECT CAST(geolocation_zip_code_prefix AS INT) AS zip_code_prefix,
         lower(trim(geolocation_city)) AS city,
         upper(trim(geolocation_state)) AS state,
         avg(CAST(geolocation_lat AS DOUBLE)) AS latitude,
         avg(CAST(geolocation_lng AS DOUBLE)) AS longitude,
         count(*) AS point_count
  FROM {b}.olist_geolocation
  WHERE geolocation_zip_code_prefix IS NOT NULL
  GROUP BY CAST(geolocation_zip_code_prefix AS INT),
           lower(trim(geolocation_city)), upper(trim(geolocation_state))
), ranked AS (
  SELECT *, row_number() OVER (
    PARTITION BY zip_code_prefix ORDER BY point_count DESC, state, city
  ) AS choice_rank
  FROM candidates
)
SELECT zip_code_prefix, city, state, latitude, longitude, point_count
FROM ranked WHERE choice_rank = 1
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {s}.fact_orders AS
SELECT DISTINCT order_id, customer_id, lower(trim(order_status)) AS order_status,
       to_timestamp(order_purchase_timestamp) AS purchased_at,
       to_timestamp(order_approved_at) AS approved_at,
       to_timestamp(order_delivered_carrier_date) AS carrier_received_at,
       to_timestamp(order_delivered_customer_date) AS delivered_at,
       to_timestamp(order_estimated_delivery_date) AS estimated_delivery_at
FROM {b}.olist_orders
WHERE order_id IS NOT NULL AND customer_id IS NOT NULL AND order_purchase_timestamp IS NOT NULL
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {s}.fact_order_items AS
SELECT order_id, CAST(order_item_id AS INT) AS order_item_id, product_id, seller_id,
       to_timestamp(shipping_limit_date) AS shipping_limit_at,
       CAST(price AS DECIMAL(18,2)) AS item_value_brl,
       CAST(freight_value AS DECIMAL(18,2)) AS freight_value_brl
FROM {b}.olist_order_items
WHERE order_id IS NOT NULL AND product_id IS NOT NULL AND seller_id IS NOT NULL
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {s}.fact_payments AS
SELECT order_id, CAST(payment_sequential AS INT) AS payment_sequence,
       upper(trim(payment_type)) AS payment_type,
       CAST(payment_installments AS INT) AS installments,
       CAST(payment_value AS DECIMAL(18,2)) AS payment_value_brl
FROM {b}.olist_order_payments
WHERE order_id IS NOT NULL AND CAST(payment_value AS DECIMAL(18,2)) >= 0
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {s}.fact_reviews AS
SELECT review_id, order_id, CAST(review_score AS INT) AS review_score,
       nullif(trim(review_comment_title), '') AS review_title,
       nullif(trim(review_comment_message), '') AS review_message,
       to_timestamp(review_creation_date) AS review_created_at,
       to_timestamp(review_answer_timestamp) AS review_answered_at
FROM {b}.olist_order_reviews
WHERE review_id IS NOT NULL AND order_id IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY review_id, order_id ORDER BY _ingested_at DESC) = 1
""")

for table in ("dim_customers", "dim_sellers", "dim_products", "dim_geolocation_zip", "fact_orders", "fact_order_items", "fact_payments", "fact_reviews"):
    count = spark.table(f"{catalog}.{base_schema}_silver.{table}").count()
    print(f"{table}: {count:,} rows")

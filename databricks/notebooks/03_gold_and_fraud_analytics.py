# Databricks notebook source
# MAGIC %md
# MAGIC # Gold commerce KPIs and explainable payment-risk signals
# MAGIC Olist has no verified fraud label. `payment_risk_signals` is an anomaly triage table, not a claim of fraud and not a supervised model evaluation.

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
s = f"`{catalog}`.`{base_schema}_silver`"
g = f"`{catalog}`.`{base_schema}_gold`"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.daily_commerce_kpis AS
WITH order_payments AS (
  SELECT order_id, sum(payment_value_brl) AS revenue_brl
  FROM {s}.fact_payments GROUP BY order_id
)
SELECT CAST(o.purchased_at AS DATE) AS order_date,
       count(*) AS order_count,
       count(DISTINCT c.customer_unique_id) AS unique_customers,
       CAST(sum(coalesce(p.revenue_brl, 0)) AS DECIMAL(18,2)) AS revenue_brl,
       CAST(avg(coalesce(p.revenue_brl, 0)) AS DECIMAL(18,2)) AS average_order_value_brl,
       sum(CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
       sum(CASE WHEN o.order_status IN ('canceled', 'unavailable') THEN 1 ELSE 0 END) AS failed_orders
FROM {s}.fact_orders o
JOIN {s}.dim_customers c USING (customer_id)
LEFT JOIN order_payments p USING (order_id)
GROUP BY CAST(o.purchased_at AS DATE)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.delivery_performance AS
SELECT c.state AS customer_state,
       count(*) AS delivered_orders,
       round(avg(timestampdiff(HOUR, o.purchased_at, o.delivered_at)) / 24.0, 2) AS average_delivery_days,
       round(100.0 * avg(CASE WHEN o.delivered_at <= o.estimated_delivery_at THEN 1 ELSE 0 END), 2) AS on_time_delivery_pct
FROM {s}.fact_orders o
JOIN {s}.dim_customers c USING (customer_id)
WHERE o.order_status = 'delivered' AND o.delivered_at IS NOT NULL
GROUP BY c.state
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.seller_performance AS
WITH item_metrics AS (
  SELECT seller_id, count(DISTINCT order_id) AS orders,
         CAST(sum(item_value_brl) AS DECIMAL(18,2)) AS merchandise_value_brl,
         CAST(sum(freight_value_brl) AS DECIMAL(18,2)) AS freight_value_brl
  FROM {s}.fact_order_items GROUP BY seller_id
), seller_reviews AS (
  SELECT i.seller_id, avg(r.review_score) AS average_review_score
  FROM (SELECT DISTINCT order_id, seller_id FROM {s}.fact_order_items) i
  JOIN {s}.fact_reviews r USING (order_id)
  GROUP BY i.seller_id
)
SELECT d.seller_id, d.city, d.state, m.orders, m.merchandise_value_brl,
       m.freight_value_brl, round(r.average_review_score, 2) AS average_review_score
FROM {s}.dim_sellers d
JOIN item_metrics m USING (seller_id)
LEFT JOIN seller_reviews r USING (seller_id)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.customer_360 AS
WITH order_value AS (
  SELECT o.order_id, o.customer_id, o.purchased_at, o.order_status,
         sum(coalesce(p.payment_value_brl, 0)) AS order_value_brl
  FROM {s}.fact_orders o LEFT JOIN {s}.fact_payments p USING (order_id)
  GROUP BY o.order_id, o.customer_id, o.purchased_at, o.order_status
)
SELECT c.customer_unique_id, max(c.state) AS state,
       count(*) AS order_count, min(o.purchased_at) AS first_order_at,
       max(o.purchased_at) AS latest_order_at,
       CAST(sum(o.order_value_brl) AS DECIMAL(18,2)) AS lifetime_value_brl,
       CAST(avg(o.order_value_brl) AS DECIMAL(18,2)) AS average_order_value_brl,
       sum(CASE WHEN o.order_status IN ('canceled', 'unavailable') THEN 1 ELSE 0 END) AS failed_orders
FROM order_value o JOIN {s}.dim_customers c USING (customer_id)
GROUP BY c.customer_unique_id
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {g}.payment_risk_signals AS
WITH payment_context AS (
  SELECT o.order_id, c.customer_unique_id, o.purchased_at, o.order_status,
         p.payment_sequence, p.payment_type, p.installments, p.payment_value_brl,
         avg(p.payment_value_brl) OVER (PARTITION BY c.customer_unique_id) AS customer_average_brl,
         max(r.review_score) AS review_score
  FROM {s}.fact_orders o
  JOIN {s}.dim_customers c USING (customer_id)
  JOIN {s}.fact_payments p USING (order_id)
  LEFT JOIN {s}.fact_reviews r USING (order_id)
  GROUP BY o.order_id, c.customer_unique_id, o.purchased_at, o.order_status,
           p.payment_sequence, p.payment_type, p.installments, p.payment_value_brl
), scored AS (
  SELECT *,
    CASE WHEN payment_value_brl > greatest(customer_average_brl * 3, 500) THEN 35 ELSE 0 END
    + CASE WHEN installments >= 10 THEN 20 ELSE 0 END
    + CASE WHEN payment_sequence > 1 THEN 10 ELSE 0 END
    + CASE WHEN order_status IN ('canceled', 'unavailable') THEN 25 ELSE 0 END
    + CASE WHEN review_score = 1 THEN 10 ELSE 0 END AS risk_score,
    filter(array(
      CASE WHEN payment_value_brl > greatest(customer_average_brl * 3, 500) THEN 'AMOUNT_OUTLIER' END,
      CASE WHEN installments >= 10 THEN 'HIGH_INSTALLMENTS' END,
      CASE WHEN payment_sequence > 1 THEN 'SPLIT_PAYMENT' END,
      CASE WHEN order_status IN ('canceled', 'unavailable') THEN 'FAILED_ORDER' END,
      CASE WHEN review_score = 1 THEN 'LOW_REVIEW' END
    ), reason -> reason IS NOT NULL) AS reason_codes
  FROM payment_context
)
SELECT *, CASE WHEN risk_score >= 60 THEN 'HIGH'
               WHEN risk_score >= 30 THEN 'MEDIUM'
               ELSE 'LOW' END AS risk_band,
       'HEURISTIC_PROXY_NOT_CONFIRMED_FRAUD' AS label_status
FROM scored
""")

for table in ("daily_commerce_kpis", "delivery_performance", "seller_performance", "customer_360", "payment_risk_signals"):
    count = spark.table(f"{catalog}.{base_schema}_gold.{table}").count()
    print(f"{table}: {count:,} rows")

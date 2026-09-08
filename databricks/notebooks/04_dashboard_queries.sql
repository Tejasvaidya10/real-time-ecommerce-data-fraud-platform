-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AI/BI dashboard datasets
-- MAGIC Run each query, save it as a dashboard dataset, and build KPI, line, bar, and table visuals. Replace `workspace` only if the setup widget used another catalog.

-- COMMAND ----------

SELECT
  sum(order_count) AS orders,
  (SELECT count(*) FROM workspace.ecommerce_gold.customer_360) AS unique_customers,
  round(sum(revenue_brl), 2) AS revenue_brl,
  round(sum(revenue_brl) / sum(order_count), 2) AS average_order_value_brl,
  round(100.0 * sum(delivered_orders) / sum(order_count), 2) AS delivered_pct
FROM workspace.ecommerce_gold.daily_commerce_kpis;

-- COMMAND ----------

SELECT order_date, order_count, revenue_brl, average_order_value_brl
FROM workspace.ecommerce_gold.daily_commerce_kpis
ORDER BY order_date;

-- COMMAND ----------

SELECT risk_band, count(*) AS payments,
       round(sum(payment_value_brl), 2) AS payment_value_brl,
       round(avg(risk_score), 2) AS average_risk_score
FROM workspace.ecommerce_gold.payment_risk_signals
GROUP BY risk_band
ORDER BY CASE risk_band WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END;

-- COMMAND ----------

SELECT state, city, orders, merchandise_value_brl, average_review_score
FROM workspace.ecommerce_gold.seller_performance
ORDER BY merchandise_value_brl DESC
LIMIT 25;

-- COMMAND ----------

SELECT customer_state, delivered_orders, average_delivery_days, on_time_delivery_pct
FROM workspace.ecommerce_gold.delivery_performance
ORDER BY delivered_orders DESC;

-- COMMAND ----------

SELECT order_id, purchased_at, payment_type, payment_value_brl,
       risk_score, risk_band, reason_codes, label_status
FROM workspace.ecommerce_gold.payment_risk_signals
WHERE risk_band IN ('HIGH', 'MEDIUM')
ORDER BY risk_score DESC, payment_value_brl DESC
LIMIT 100;

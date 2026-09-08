# Databricks notebook source
# MAGIC %md
# MAGIC # Olist lakehouse setup
# MAGIC Creates Bronze, Silver, and Gold schemas plus a Unity Catalog volume for the nine original CSV files.

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

for layer in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{base_schema}_{layer}`")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{base_schema}_bronze`.`olist_raw`")
source_path = f"/Volumes/{catalog}/{base_schema}_bronze/olist_raw"
print(f"Upload all nine Olist CSV files to: {source_path}")

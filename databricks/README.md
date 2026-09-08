# Databricks Free Edition walkthrough

These are Databricks source-format notebooks. They keep the cloud analytics path reproducible in Git while the Kafka/Debezium real-time path runs locally at zero cost.

## Run order

1. In Databricks Free Edition, create a Git folder from this GitHub repository, or import the files in `databricks/notebooks`.
2. Run `00_setup.py`. It creates three schemas and the `olist_raw` Unity Catalog volume.
3. In Catalog Explorer, open `<catalog>.<base_schema>_bronze.olist_raw`, choose **Upload to this volume**, and upload the nine CSV files from `data/olist/archive` on your Mac.
4. Run `01_bronze_olist.py`, `02_silver_olist.py`, and `03_gold_and_fraud_analytics.py` in order.
5. Run `04_dashboard_queries.sql`. Use its result tables as datasets in an AI/BI dashboard.

The default catalog is the notebook's current catalog and the default base schema is `ecommerce`. Change the widgets only if those names conflict with something in your workspace.

## Why this is separate from local Kafka

Free Edition is ideal for a portfolio-scale Delta Lake and SQL analytics demonstration, but it is not the always-on infrastructure for this project. The local Docker path proves CDC, Kafka, streaming, replay safety, and real-time fraud decisions. The Databricks path proves medallion modeling, managed Delta tables, SQL analytics, and dashboard-ready datasets without creating a cloud bill.

## Fraud-label limitation

Olist publishes order, payment, delivery, and review data, but no verified fraud or chargeback label. The `gold.payment_risk_signals` table therefore produces explainable anomaly signals and a `risk_band`; it does not claim that a payment is fraudulent. Supervised fraud precision/recall remains measurable only on the synthetic labeled stream in the local platform.

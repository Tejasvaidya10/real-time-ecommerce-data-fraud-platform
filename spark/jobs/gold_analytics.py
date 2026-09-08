from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import build_spark, env
from src.gold.contracts import GOLD_TABLE_BY_NAME
from src.gold.dashboard_export import validate_dashboard_payload


def read_delta(spark, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_gold(frame: DataFrame, path: str) -> None:
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Cannot serialize {type(value).__name__} to dashboard JSON")


def write_dashboard_export(payload: dict, output_path: str) -> None:
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(f"{target.suffix}.tmp")
    temporary.write_text(
        json.dumps(payload, default=json_value, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(target)


def main() -> None:
    spark = build_spark("ecommerce-gold-analytics")
    spark.sparkContext.setLogLevel("WARN")
    lakehouse_root = env("LAKEHOUSE_ROOT", "/opt/project/data/lakehouse")
    dashboard_export_path = env(
        "DASHBOARD_EXPORT_PATH",
        "/opt/project/data/exports/dashboard.json",
    )
    refreshed_at = datetime.now(timezone.utc)

    customers = read_delta(spark, f"{lakehouse_root}/silver/customers")
    orders = read_delta(spark, f"{lakehouse_root}/silver/orders")
    payments = read_delta(spark, f"{lakehouse_root}/silver/payments")
    decisions = read_delta(spark, f"{lakehouse_root}/silver/fraud_decisions")
    chargebacks = read_delta(spark, f"{lakehouse_root}/silver/chargebacks")

    duplicate_decisions = (
        decisions.groupBy("transaction_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    unmatched_chargebacks = chargebacks.join(
        payments.select("transaction_id"),
        "transaction_id",
        "left_anti",
    ).count()
    if duplicate_decisions:
        raise RuntimeError(
            f"Cannot build Gold with {duplicate_decisions} duplicate fraud decisions"
        )
    if unmatched_chargebacks:
        raise RuntimeError(
            f"Cannot build Gold with {unmatched_chargebacks} unmatched chargebacks"
        )

    enriched = (
        payments.join(
            decisions.select(
                "transaction_id",
                "risk_score",
                "action",
                "reason_codes",
                "rules_version",
                "decision_time",
            ),
            "transaction_id",
            "left",
        )
        .join(
            chargebacks.select(
                "transaction_id",
                F.col("chargeback_id").alias("confirmed_chargeback_id"),
                F.col("reason").alias("chargeback_reason"),
                "reported_at",
            ),
            "transaction_id",
            "left",
        )
        .withColumn("metric_date", F.to_date("event_time"))
        .cache()
    )

    missing_decisions = enriched.filter(F.col("action").isNull()).count()
    if missing_decisions:
        enriched.unpersist()
        raise RuntimeError(
            f"Cannot build Gold while {missing_decisions} payments lack fraud decisions"
        )

    daily_business_kpis = (
        enriched.groupBy("metric_date", "currency")
        .agg(
            F.count("transaction_id").alias("payment_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("seller_id").alias("active_sellers"),
            F.sum("amount").alias("gross_payment_value"),
            F.avg("amount").cast("decimal(18,2)").alias("average_payment_value"),
            F.sum(
                F.when(F.col("confirmed_chargeback_id").isNotNull(), 1).otherwise(0)
            ).alias("chargeback_count"),
            F.sum(
                F.when(
                    F.col("confirmed_chargeback_id").isNotNull(),
                    F.col("amount"),
                ).otherwise(F.lit(0))
            ).alias("chargeback_amount"),
        )
        .withColumn(
            "chargeback_rate_pct",
            F.round(F.col("chargeback_count") * 100.0 / F.col("payment_count"), 2),
        )
        .withColumn("refreshed_at", F.lit(refreshed_at).cast("timestamp"))
    )

    daily_fraud_kpis = (
        enriched.groupBy("metric_date", "rules_version")
        .agg(
            F.count("transaction_id").alias("total_decisions"),
            F.sum(F.when(F.col("action") == "APPROVE", 1).otherwise(0)).alias(
                "approved_count"
            ),
            F.sum(F.when(F.col("action") == "REVIEW", 1).otherwise(0)).alias(
                "review_count"
            ),
            F.sum(F.when(F.col("action") == "DECLINE", 1).otherwise(0)).alias(
                "declined_count"
            ),
            F.avg("risk_score").cast("decimal(8,2)").alias("average_risk_score"),
            F.sum(
                F.when(F.col("action") == "REVIEW", F.col("amount")).otherwise(0)
            ).alias("review_amount"),
            F.sum(
                F.when(F.col("action") == "DECLINE", F.col("amount")).otherwise(0)
            ).alias("declined_amount"),
            F.sum(
                F.when(F.col("confirmed_chargeback_id").isNotNull(), 1).otherwise(0)
            ).alias("confirmed_fraud_count"),
            F.sum(
                F.when(
                    F.col("confirmed_chargeback_id").isNotNull(),
                    F.col("amount"),
                ).otherwise(0)
            ).alias("confirmed_fraud_amount"),
        )
        .withColumn(
            "flagged_rate_pct",
            F.round(
                (F.col("review_count") + F.col("declined_count"))
                * 100.0
                / F.col("total_decisions"),
                2,
            ),
        )
        .withColumn(
            "confirmed_fraud_rate_pct",
            F.round(
                F.col("confirmed_fraud_count")
                * 100.0
                / F.col("total_decisions"),
                2,
            ),
        )
        .withColumn("refreshed_at", F.lit(refreshed_at).cast("timestamp"))
    )

    seller_performance = (
        enriched.groupBy("seller_id")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("amount").alias("gross_payment_value"),
            F.avg("risk_score").cast("decimal(8,2)").alias("average_risk_score"),
            F.sum(F.when(F.col("action") == "REVIEW", 1).otherwise(0)).alias(
                "review_count"
            ),
            F.sum(F.when(F.col("action") == "DECLINE", 1).otherwise(0)).alias(
                "declined_count"
            ),
            F.sum(
                F.when(F.col("confirmed_chargeback_id").isNotNull(), 1).otherwise(0)
            ).alias("confirmed_chargeback_count"),
            F.max("event_time").alias("latest_payment_at"),
        )
        .withColumn(
            "flagged_rate_pct",
            F.round(
                (F.col("review_count") + F.col("declined_count"))
                * 100.0
                / F.col("transaction_count"),
                2,
            ),
        )
        .withColumn(
            "chargeback_rate_pct",
            F.round(
                F.col("confirmed_chargeback_count")
                * 100.0
                / F.col("transaction_count"),
                2,
            ),
        )
        .withColumn("refreshed_at", F.lit(refreshed_at).cast("timestamp"))
    )

    customer_orders = orders.groupBy("customer_id").agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount").alias("lifetime_order_value"),
        F.max("created_at").alias("latest_order_at"),
    )
    customer_payments = enriched.groupBy("customer_id").agg(
        F.count("transaction_id").alias("payment_count"),
        F.sum("amount").alias("lifetime_payment_value"),
        F.max("event_time").alias("latest_payment_at"),
        F.avg("risk_score").cast("decimal(8,2)").alias("average_risk_score"),
        F.sum(F.when(F.col("action") == "REVIEW", 1).otherwise(0)).alias(
            "review_count"
        ),
        F.sum(F.when(F.col("action") == "DECLINE", 1).otherwise(0)).alias(
            "declined_count"
        ),
        F.sum(
            F.when(F.col("confirmed_chargeback_id").isNotNull(), 1).otherwise(0)
        ).alias("confirmed_chargeback_count"),
    )
    customer_360 = (
        customers.select(
            "customer_id",
            "home_region",
            "account_created_at",
            "average_order_amount",
        )
        .join(customer_orders, "customer_id", "left")
        .join(customer_payments, "customer_id", "left")
        .fillna(
            0,
            subset=[
                "order_count",
                "lifetime_order_value",
                "payment_count",
                "lifetime_payment_value",
                "average_risk_score",
                "review_count",
                "declined_count",
                "confirmed_chargeback_count",
            ],
        )
        .withColumn(
            "customer_risk_segment",
            F.when(
                (F.col("confirmed_chargeback_count") > 0)
                | (F.col("declined_count") > 0),
                "HIGH",
            )
            .when(
                (F.col("review_count") > 0) | (F.col("average_risk_score") >= 20),
                "MEDIUM",
            )
            .otherwise("LOW"),
        )
        .withColumn("refreshed_at", F.lit(refreshed_at).cast("timestamp"))
    )

    tables = {
        "daily_business_kpis": daily_business_kpis,
        "daily_fraud_kpis": daily_fraud_kpis,
        "seller_performance": seller_performance,
        "customer_360": customer_360,
    }
    if set(tables) != set(GOLD_TABLE_BY_NAME):
        raise RuntimeError("Gold table implementation does not match its contracts")

    row_counts = {}
    published_tables = {}
    for table_name, frame in tables.items():
        output_path = f"{lakehouse_root}/gold/{table_name}"
        write_gold(frame, output_path)
        published_tables[table_name] = read_delta(spark, output_path)
        row_counts[table_name] = published_tables[table_name].count()

    enriched.unpersist()
    summary = {
        "refreshed_at": refreshed_at.isoformat(),
        "bronze_records": read_delta(
            spark, f"{lakehouse_root}/bronze/kafka_events"
        ).count(),
        "quarantine_records": read_delta(
            spark, f"{lakehouse_root}/quarantine/cdc_records"
        ).count(),
        "source_customers": customers.count(),
        "source_orders": orders.count(),
        "source_payments": payments.count(),
        "source_decisions": decisions.count(),
        "source_chargebacks": chargebacks.count(),
        "gold_row_counts": dict(sorted(row_counts.items())),
    }
    seller_watchlist = []
    seller_rows = (
        published_tables["seller_performance"]
        .orderBy(
            F.col("confirmed_chargeback_count").desc(),
            F.col("flagged_rate_pct").desc(),
            F.col("gross_payment_value").desc(),
        )
        .drop("seller_id", "latest_payment_at", "refreshed_at")
        .limit(8)
        .collect()
    )
    for rank, row in enumerate(seller_rows, start=1):
        seller_watchlist.append({"seller_rank": rank, **row.asDict(recursive=True)})

    customer_risk_segments = [
        row.asDict(recursive=True)
        for row in (
            published_tables["customer_360"]
            .groupBy("customer_risk_segment")
            .count()
            .collect()
        )
    ]
    dashboard_payload = {
        "metadata": summary,
        "daily_business_kpis": [
            row.asDict(recursive=True)
            for row in published_tables["daily_business_kpis"]
            .drop("refreshed_at")
            .collect()
        ],
        "daily_fraud_kpis": [
            row.asDict(recursive=True)
            for row in published_tables["daily_fraud_kpis"]
            .drop("refreshed_at")
            .collect()
        ],
        "seller_risk_watchlist": seller_watchlist,
        "customer_risk_segments": customer_risk_segments,
    }
    validate_dashboard_payload(dashboard_payload)
    write_dashboard_export(dashboard_payload, dashboard_export_path)
    print(f"GOLD_SUMMARY={json.dumps(summary, sort_keys=True)}")
    spark.stop()


if __name__ == "__main__":
    main()

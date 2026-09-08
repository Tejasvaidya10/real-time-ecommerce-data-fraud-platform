from __future__ import annotations

import json

from pyspark.sql import functions as F

from common import build_spark, env
from src.gold.contracts import GOLD_TABLE_SPECS
from src.silver.contracts import ENTITY_SPECS


PAYMENTS_TOPIC = "commerce.commerce.payments"


def main() -> None:
    spark = build_spark("ecommerce-pipeline-verification")
    spark.sparkContext.setLogLevel("ERROR")
    lakehouse_root = env("LAKEHOUSE_ROOT", "/opt/project/data/lakehouse")

    bronze = spark.read.format("delta").load(f"{lakehouse_root}/bronze/kafka_events")
    decisions = spark.read.format("delta").load(f"{lakehouse_root}/silver/fraud_decisions")

    topic_counts = {
        row["topic"]: row["count"]
        for row in bronze.groupBy("topic").count().collect()
    }
    action_counts = {
        row["action"]: row["count"]
        for row in decisions.groupBy("action").count().collect()
    }
    reason_counts = {
        row["reason"]: row["count"]
        for row in (
            decisions.select(F.explode("reason_codes").alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        )
    }
    decision_count = decisions.count()
    duplicate_decision_ids = (
        decisions.groupBy("decision_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    silver_counts = {}
    silver_duplicate_keys = {}
    silver_tables = {}
    for spec in ENTITY_SPECS:
        table = spark.read.format("delta").load(
            f"{lakehouse_root}/silver/{spec.name}"
        )
        silver_tables[spec.name] = table
        silver_counts[spec.name] = table.count()
        silver_duplicate_keys[spec.name] = (
            table.groupBy(spec.primary_key)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
    quarantine_records = (
        spark.read.format("delta")
        .load(f"{lakehouse_root}/quarantine/cdc_records")
        .count()
    )
    gold_counts = {}
    gold_duplicate_keys = {}
    gold_refresh_timestamps = {}
    gold_tables = {}
    for spec in GOLD_TABLE_SPECS:
        table = spark.read.format("delta").load(
            f"{lakehouse_root}/gold/{spec.name}"
        )
        gold_tables[spec.name] = table
        gold_counts[spec.name] = table.count()
        gold_duplicate_keys[spec.name] = (
            table.groupBy(*spec.primary_key)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        refresh_range = table.agg(
            F.min("refreshed_at").alias("minimum"),
            F.max("refreshed_at").alias("maximum"),
        ).first()
        if refresh_range["minimum"] != refresh_range["maximum"]:
            raise RuntimeError(f"Gold table {spec.name} contains mixed refresh timestamps")
        if refresh_range["minimum"] is not None:
            gold_refresh_timestamps[spec.name] = refresh_range["minimum"].isoformat()

    payment_count = topic_counts.get(PAYMENTS_TOPIC, 0)
    if decision_count != payment_count:
        raise RuntimeError(
            f"Expected one decision per payment: payments={payment_count}, decisions={decision_count}"
        )
    if duplicate_decision_ids:
        raise RuntimeError(f"Found {duplicate_decision_ids} duplicated decision IDs")
    if any(silver_duplicate_keys.values()):
        raise RuntimeError(f"Found duplicated Silver primary keys: {silver_duplicate_keys}")
    if silver_counts["payments"] != decision_count:
        raise RuntimeError(
            "Expected matching current payment and decision counts: "
            f"payments={silver_counts['payments']}, decisions={decision_count}"
        )
    if any(gold_duplicate_keys.values()):
        raise RuntimeError(f"Found duplicated Gold grains: {gold_duplicate_keys}")
    if len(set(gold_refresh_timestamps.values())) > 1:
        raise RuntimeError(
            f"Gold tables are from different refreshes: {gold_refresh_timestamps}"
        )

    business_payment_count = (
        gold_tables["daily_business_kpis"]
        .agg(F.sum("payment_count").alias("count"))
        .first()["count"]
        or 0
    )
    fraud_decision_count = (
        gold_tables["daily_fraud_kpis"]
        .agg(F.sum("total_decisions").alias("count"))
        .first()["count"]
        or 0
    )
    confirmed_fraud_count = (
        gold_tables["daily_fraud_kpis"]
        .agg(F.sum("confirmed_fraud_count").alias("count"))
        .first()["count"]
        or 0
    )
    seller_payment_count = (
        gold_tables["seller_performance"]
        .agg(F.sum("transaction_count").alias("count"))
        .first()["count"]
        or 0
    )
    customer_payment_count = (
        gold_tables["customer_360"]
        .agg(F.sum("payment_count").alias("count"))
        .first()["count"]
        or 0
    )
    if business_payment_count != silver_counts["payments"]:
        raise RuntimeError("Gold business payment totals do not reconcile to Silver")
    if fraud_decision_count != decision_count:
        raise RuntimeError("Gold fraud decision totals do not reconcile to Silver")
    if confirmed_fraud_count != silver_counts["chargebacks"]:
        raise RuntimeError("Gold confirmed fraud totals do not reconcile to chargebacks")
    if seller_payment_count != silver_counts["payments"]:
        raise RuntimeError("Gold seller payment totals do not reconcile to Silver")
    if customer_payment_count != silver_counts["payments"]:
        raise RuntimeError("Gold customer payment totals do not reconcile to Silver")
    if gold_counts["customer_360"] != silver_counts["customers"]:
        raise RuntimeError("Customer 360 does not contain one row per Silver customer")
    expected_sellers = silver_tables["payments"].select("seller_id").distinct().count()
    if gold_counts["seller_performance"] != expected_sellers:
        raise RuntimeError("Seller performance does not contain one row per active seller")

    summary = {
        "bronze_records": sum(topic_counts.values()),
        "topic_counts": dict(sorted(topic_counts.items())),
        "fraud_decisions": decision_count,
        "action_counts": dict(sorted(action_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "duplicate_decision_ids": duplicate_decision_ids,
        "silver_counts": dict(sorted(silver_counts.items())),
        "silver_duplicate_keys": dict(sorted(silver_duplicate_keys.items())),
        "gold_counts": dict(sorted(gold_counts.items())),
        "gold_duplicate_keys": dict(sorted(gold_duplicate_keys.items())),
        "gold_refreshed_at": next(iter(gold_refresh_timestamps.values()), None),
        "quarantine_records": quarantine_records,
    }
    print(f"PIPELINE_SUMMARY={json.dumps(summary, sort_keys=True)}")
    spark.stop()


if __name__ == "__main__":
    main()

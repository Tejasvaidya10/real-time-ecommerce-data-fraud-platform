from __future__ import annotations

import json

from pyspark.sql import functions as F

from common import build_spark, env


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

    payment_count = topic_counts.get(PAYMENTS_TOPIC, 0)
    if decision_count != payment_count:
        raise RuntimeError(
            f"Expected one decision per payment: payments={payment_count}, decisions={decision_count}"
        )
    if duplicate_decision_ids:
        raise RuntimeError(f"Found {duplicate_decision_ids} duplicated decision IDs")

    summary = {
        "bronze_records": sum(topic_counts.values()),
        "topic_counts": dict(sorted(topic_counts.items())),
        "fraud_decisions": decision_count,
        "action_counts": dict(sorted(action_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "duplicate_decision_ids": duplicate_decision_ids,
    }
    print(f"PIPELINE_SUMMARY={json.dumps(summary, sort_keys=True)}")
    spark.stop()


if __name__ == "__main__":
    main()

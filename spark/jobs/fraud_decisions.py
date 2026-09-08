from __future__ import annotations

from pyspark.sql import functions as F

from common import build_spark, env


PAYMENTS_TOPIC = "commerce.commerce.payments"


def main() -> None:
    spark = build_spark("ecommerce-fraud-decisions")
    spark.sparkContext.setLogLevel("WARN")
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    lakehouse_root = env("LAKEHOUSE_ROOT", "/opt/project/data/lakehouse")
    checkpoint_root = env("CHECKPOINT_ROOT", "/opt/project/data/checkpoints")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", PAYMENTS_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.col("value").cast("string").alias("json"), F.col("timestamp").alias("kafka_timestamp"))
    )

    payment = raw.select(
        F.get_json_object("json", "$.after.transaction_id").alias("transaction_id"),
        F.get_json_object("json", "$.after.event_id").alias("event_id"),
        F.get_json_object("json", "$.after.customer_id").alias("customer_id"),
        F.get_json_object("json", "$.after.event_time").cast("long").alias("event_time_micros"),
        F.get_json_object("json", "$.after.amount").cast("double").alias("amount"),
        F.get_json_object("json", "$.after.customer_average_amount").cast("double").alias("customer_average_amount"),
        F.get_json_object("json", "$.after.is_new_device").cast("boolean").alias("is_new_device"),
        F.get_json_object("json", "$.after.ip_region").alias("ip_region"),
        F.get_json_object("json", "$.after.home_region").alias("home_region"),
        F.get_json_object("json", "$.after.shipping_region").alias("shipping_region"),
        F.get_json_object("json", "$.after.login_failures").cast("int").alias("login_failures"),
        F.get_json_object("json", "$.after.attempt_number").cast("int").alias("attempt_number"),
        F.get_json_object("json", "$.op").alias("cdc_operation"),
        "kafka_timestamp",
    ).filter(F.col("cdc_operation").isin("c", "r") & F.col("transaction_id").isNotNull())

    scored = (
        payment.withColumn(
            "reason_codes",
            F.array_compact(
                F.array(
                    F.when(F.col("amount") >= F.greatest(F.lit(250.0), F.col("customer_average_amount") * 3), F.lit("AMOUNT_OUTLIER")),
                    F.when(F.col("is_new_device"), F.lit("NEW_DEVICE")),
                    F.when(F.col("ip_region") != F.col("home_region"), F.lit("IP_HOME_MISMATCH")),
                    F.when(F.col("shipping_region") != F.col("home_region"), F.lit("SHIPPING_HOME_MISMATCH")),
                    F.when(F.col("login_failures") >= 3, F.lit("REPEATED_LOGIN_FAILURES")),
                    F.when(F.col("attempt_number") >= 4, F.lit("REPEATED_PAYMENT_ATTEMPTS")),
                )
            ),
        )
        .withColumn(
            "risk_score",
            F.least(
                F.lit(100),
                F.when(F.array_contains("reason_codes", "AMOUNT_OUTLIER"), 30).otherwise(0)
                + F.when(F.array_contains("reason_codes", "NEW_DEVICE"), 20).otherwise(0)
                + F.when(F.array_contains("reason_codes", "IP_HOME_MISMATCH"), 20).otherwise(0)
                + F.when(F.array_contains("reason_codes", "SHIPPING_HOME_MISMATCH"), 10).otherwise(0)
                + F.when(F.array_contains("reason_codes", "REPEATED_LOGIN_FAILURES"), 20).otherwise(0)
                + F.when(F.array_contains("reason_codes", "REPEATED_PAYMENT_ATTEMPTS"), 15).otherwise(0),
            ),
        )
        .withColumn(
            "action",
            F.when(F.col("risk_score") >= 60, "DECLINE")
            .when(F.col("risk_score") >= 30, "REVIEW")
            .otherwise("APPROVE"),
        )
        .withColumn("decision_id", F.sha2(F.concat_ws("|", "transaction_id", F.lit("rules-v1")), 256))
        .withColumn("rules_version", F.lit("rules-v1"))
        .withColumn("decision_time", F.current_timestamp())
        .select(
            "decision_id", "transaction_id", "customer_id", "risk_score", "action",
            "reason_codes", "rules_version", "decision_time", "kafka_timestamp",
        )
    )

    query = (
        scored.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_root}/fraud-decisions")
        .trigger(processingTime="5 seconds")
        .start(f"{lakehouse_root}/silver/fraud_decisions")
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

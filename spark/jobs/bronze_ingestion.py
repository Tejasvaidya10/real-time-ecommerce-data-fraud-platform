from __future__ import annotations

from pyspark.sql import functions as F

from common import build_spark, env


TOPICS = {
    "payments": "commerce.commerce.payments",
    "orders": "commerce.commerce.orders",
    "chargebacks": "commerce.commerce.chargebacks",
}


def main() -> None:
    spark = build_spark("ecommerce-bronze-ingestion")
    spark.sparkContext.setLogLevel("WARN")
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    lakehouse_root = env("LAKEHOUSE_ROOT", "/opt/project/data/lakehouse")
    checkpoint_root = env("CHECKPOINT_ROOT", "/opt/project/data/checkpoints")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", ",".join(TOPICS.values()))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )

    bronze = raw.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_payload"),
        F.current_timestamp().alias("ingested_at"),
        F.to_date("timestamp").alias("ingest_date"),
    )

    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_root}/bronze")
        .partitionBy("ingest_date")
        .trigger(processingTime="5 seconds")
        .start(f"{lakehouse_root}/bronze/kafka_events")
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

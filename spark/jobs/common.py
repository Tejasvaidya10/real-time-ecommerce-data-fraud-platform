from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )
    # Delta and Kafka artifacts are supplied by spark-submit --packages in Compose.
    return builder.getOrCreate()


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)

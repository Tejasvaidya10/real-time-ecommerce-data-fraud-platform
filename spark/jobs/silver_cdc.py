from __future__ import annotations

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import build_spark, env
from src.silver.contracts import ENTITY_SPECS, EntitySpec, FieldSpec


TECHNICAL_COLUMNS = (
    "_cdc_operation",
    "_source_lsn",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp",
    "_processed_at",
)


def json_path(json_column: str, *paths: str):
    return F.coalesce(*(F.get_json_object(json_column, path) for path in paths))


def record_value(json_column: str, field: str):
    return json_path(
        json_column,
        f"$.payload.after.{field}",
        f"$.after.{field}",
        f"$.payload.before.{field}",
        f"$.before.{field}",
    )


def cast_field(column, field: FieldSpec):
    if field.data_type == "timestamp_micros":
        return F.coalesce(
            F.timestamp_micros(column.try_cast("long")),
            F.try_to_timestamp(column),
        )
    if field.data_type == "decimal":
        return column.cast("decimal(12,2)")
    return column.cast(field.data_type)


def quality_errors(spec: EntitySpec) -> list:
    operation = F.col("_cdc_operation")
    is_upsert = operation.isin("c", "u", "r")
    errors = [
        F.when(F.col(spec.primary_key).isNull(), F.lit("MISSING_PRIMARY_KEY")),
        F.when(
            operation.isNull() | ~operation.isin("c", "u", "r", "d"),
            F.lit("INVALID_CDC_OPERATION"),
        ),
    ]

    for field in spec.fields:
        errors.append(
            F.when(
                is_upsert & F.col(field.name).isNull(),
                F.lit(f"MISSING_{field.name.upper()}"),
            )
        )
        if field.data_type == "string":
            errors.append(
                F.when(
                    is_upsert & (F.trim(F.col(field.name)) == ""),
                    F.lit(f"EMPTY_{field.name.upper()}"),
                )
            )

    if spec.name == "customers":
        errors.append(
            F.when(
                is_upsert & (F.col("average_order_amount") < 0),
                F.lit("NEGATIVE_AVERAGE_ORDER_AMOUNT"),
            )
        )
    elif spec.name == "orders":
        errors.extend(
            [
                F.when(
                    is_upsert & (F.col("total_amount") < 0),
                    F.lit("NEGATIVE_ORDER_AMOUNT"),
                ),
                F.when(
                    is_upsert
                    & ~F.col("status").isin(
                        "CREATED",
                        "PAYMENT_PENDING",
                        "PAID",
                        "FAILED",
                        "CANCELLED",
                        "SHIPPED",
                        "DELIVERED",
                    ),
                    F.lit("INVALID_ORDER_STATUS"),
                ),
            ]
        )
    elif spec.name == "payments":
        errors.extend(
            [
                F.when(
                    is_upsert & (F.col("amount") <= 0),
                    F.lit("NON_POSITIVE_PAYMENT_AMOUNT"),
                ),
                F.when(
                    is_upsert & (F.col("account_age_days") < 0),
                    F.lit("NEGATIVE_ACCOUNT_AGE"),
                ),
                F.when(
                    is_upsert & (F.col("login_failures") < 0),
                    F.lit("NEGATIVE_LOGIN_FAILURES"),
                ),
                F.when(
                    is_upsert & (F.col("attempt_number") <= 0),
                    F.lit("INVALID_ATTEMPT_NUMBER"),
                ),
                F.when(
                    is_upsert & (F.col("customer_average_amount") < 0),
                    F.lit("NEGATIVE_CUSTOMER_AVERAGE"),
                ),
                F.when(
                    is_upsert
                    & ~F.col("status").isin("AUTHORIZED", "DECLINED", "PENDING"),
                    F.lit("INVALID_PAYMENT_STATUS"),
                ),
            ]
        )

    return errors


def parse_entity(batch: DataFrame, spec: EntitySpec) -> DataFrame:
    fields = [
        cast_field(record_value("raw_payload", field.name), field).alias(field.name)
        for field in spec.fields
    ]
    parsed = batch.filter(F.col("topic") == spec.topic).select(
        *fields,
        json_path("raw_payload", "$.payload.op", "$.op").alias("_cdc_operation"),
        json_path("raw_payload", "$.payload.source.lsn", "$.source.lsn")
        .cast("long")
        .alias("_source_lsn"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("kafka_timestamp").alias("_kafka_timestamp"),
        F.current_timestamp().alias("_processed_at"),
        "topic",
        "raw_payload",
    )
    return parsed.withColumn(
        "_quality_errors",
        F.array_compact(F.array(*quality_errors(spec))),
    )


def latest_per_key(rows: DataFrame, primary_key: str) -> DataFrame:
    ordering = Window.partitionBy(primary_key).orderBy(
        F.col("_source_lsn").desc_nulls_last(),
        F.col("_kafka_timestamp").desc_nulls_last(),
        F.col("_kafka_offset").desc(),
    )
    return (
        rows.withColumn("_row_number", F.row_number().over(ordering))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def merge_entity(spark, rows: DataFrame, spec: EntitySpec, target_path: str) -> None:
    target_columns = [field.name for field in spec.fields] + list(TECHNICAL_COLUMNS)
    source = latest_per_key(
        rows.filter(F.size("_quality_errors") == 0).select(*target_columns),
        spec.primary_key,
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        source.filter(F.col("_cdc_operation") != "d").limit(0).write.format("delta").save(
            target_path
        )

    target_lsn = "coalesce(t._source_lsn, -1)"
    source_lsn = "coalesce(s._source_lsn, -1)"
    is_newer = (
        f"({source_lsn} > {target_lsn}) OR "
        f"({source_lsn} = {target_lsn} AND "
        "s._kafka_partition = t._kafka_partition AND "
        "s._kafka_offset > t._kafka_offset)"
    )
    (
        DeltaTable.forPath(spark, target_path)
        .alias("t")
        .merge(source.alias("s"), f"t.{spec.primary_key} = s.{spec.primary_key}")
        .whenMatchedDelete(condition=f"s._cdc_operation = 'd' AND ({is_newer})")
        .whenMatchedUpdateAll(condition=f"s._cdc_operation <> 'd' AND ({is_newer})")
        .whenNotMatchedInsertAll(condition="s._cdc_operation <> 'd'")
        .execute()
    )


def merge_quarantine(spark, invalid_rows: DataFrame, target_path: str) -> None:
    quarantine = (
        invalid_rows.select(
            F.sha2(
                F.concat_ws("|", "topic", "_kafka_partition", "_kafka_offset"),
                256,
            ).alias("quarantine_id"),
            "topic",
            F.col("_kafka_partition").alias("kafka_partition"),
            F.col("_kafka_offset").alias("kafka_offset"),
            F.col("_kafka_timestamp").alias("kafka_timestamp"),
            "raw_payload",
            F.col("_quality_errors").alias("error_codes"),
            F.current_timestamp().alias("quarantined_at"),
        )
        .dropDuplicates(["quarantine_id"])
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        quarantine.limit(0).write.format("delta").save(target_path)
    if not quarantine.take(1):
        return
    (
        DeltaTable.forPath(spark, target_path)
        .alias("t")
        .merge(quarantine.alias("s"), "t.quarantine_id = s.quarantine_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_batch(spark, lakehouse_root: str, batch: DataFrame, batch_id: int) -> None:
    del batch_id
    batch.persist(StorageLevel.DISK_ONLY)
    try:
        for spec in ENTITY_SPECS:
            parsed = parse_entity(batch, spec).localCheckpoint(eager=True)
            try:
                merge_entity(
                    spark,
                    parsed,
                    spec,
                    f"{lakehouse_root}/silver/{spec.name}",
                )
                merge_quarantine(
                    spark,
                    parsed.filter(F.size("_quality_errors") > 0).select(
                        "topic",
                        "_kafka_partition",
                        "_kafka_offset",
                        "_kafka_timestamp",
                        "raw_payload",
                        "_quality_errors",
                    ),
                    f"{lakehouse_root}/quarantine/cdc_records",
                )
            finally:
                parsed.unpersist()
    finally:
        batch.unpersist()


def main() -> None:
    spark = build_spark("ecommerce-silver-cdc")
    spark.sparkContext.setLogLevel("WARN")
    lakehouse_root = env("LAKEHOUSE_ROOT", "/opt/project/data/lakehouse")
    checkpoint_root = env("CHECKPOINT_ROOT", "/opt/project/data/checkpoints")

    bronze = spark.readStream.format("delta").load(
        f"{lakehouse_root}/bronze/kafka_events"
    )
    query = (
        bronze.writeStream.foreachBatch(
            lambda batch, batch_id: process_batch(
                spark,
                lakehouse_root,
                batch,
                batch_id,
            )
        )
        .option("checkpointLocation", f"{checkpoint_root}/silver-cdc")
        .trigger(processingTime="5 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GoldTableSpec:
    name: str
    grain: str
    primary_key: tuple[str, ...]


GOLD_TABLE_SPECS = (
    GoldTableSpec(
        name="daily_business_kpis",
        grain="one row per payment event date and currency",
        primary_key=("metric_date", "currency"),
    ),
    GoldTableSpec(
        name="daily_fraud_kpis",
        grain="one row per payment event date and fraud-rules version",
        primary_key=("metric_date", "rules_version"),
    ),
    GoldTableSpec(
        name="seller_performance",
        grain="one row per seller",
        primary_key=("seller_id",),
    ),
    GoldTableSpec(
        name="customer_360",
        grain="one row per customer",
        primary_key=("customer_id",),
    ),
)


GOLD_TABLE_BY_NAME = {spec.name: spec for spec in GOLD_TABLE_SPECS}

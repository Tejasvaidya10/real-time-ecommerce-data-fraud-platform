from __future__ import annotations

from dataclasses import dataclass


SUPPORTED_TYPES = frozenset({"string", "integer", "long", "boolean", "decimal", "timestamp_micros"})


@dataclass(frozen=True)
class FieldSpec:
    name: str
    data_type: str


@dataclass(frozen=True)
class EntitySpec:
    name: str
    topic: str
    primary_key: str
    fields: tuple[FieldSpec, ...]


ENTITY_SPECS = (
    EntitySpec(
        name="customers",
        topic="commerce.commerce.customers",
        primary_key="customer_id",
        fields=(
            FieldSpec("customer_id", "string"),
            FieldSpec("home_region", "string"),
            FieldSpec("account_created_at", "timestamp_micros"),
            FieldSpec("average_order_amount", "decimal"),
            FieldSpec("trusted_device_id", "string"),
            FieldSpec("updated_at", "timestamp_micros"),
        ),
    ),
    EntitySpec(
        name="orders",
        topic="commerce.commerce.orders",
        primary_key="order_id",
        fields=(
            FieldSpec("order_id", "string"),
            FieldSpec("customer_id", "string"),
            FieldSpec("status", "string"),
            FieldSpec("total_amount", "decimal"),
            FieldSpec("shipping_region", "string"),
            FieldSpec("created_at", "timestamp_micros"),
            FieldSpec("updated_at", "timestamp_micros"),
        ),
    ),
    EntitySpec(
        name="payments",
        topic="commerce.commerce.payments",
        primary_key="transaction_id",
        fields=(
            FieldSpec("transaction_id", "string"),
            FieldSpec("event_id", "string"),
            FieldSpec("order_id", "string"),
            FieldSpec("customer_id", "string"),
            FieldSpec("seller_id", "string"),
            FieldSpec("event_time", "timestamp_micros"),
            FieldSpec("amount", "decimal"),
            FieldSpec("currency", "string"),
            FieldSpec("payment_type", "string"),
            FieldSpec("device_id", "string"),
            FieldSpec("is_new_device", "boolean"),
            FieldSpec("ip_region", "string"),
            FieldSpec("home_region", "string"),
            FieldSpec("shipping_region", "string"),
            FieldSpec("account_age_days", "integer"),
            FieldSpec("login_failures", "integer"),
            FieldSpec("attempt_number", "integer"),
            FieldSpec("customer_average_amount", "decimal"),
            FieldSpec("status", "string"),
            FieldSpec("created_at", "timestamp_micros"),
        ),
    ),
    EntitySpec(
        name="chargebacks",
        topic="commerce.commerce.chargebacks",
        primary_key="chargeback_id",
        fields=(
            FieldSpec("chargeback_id", "string"),
            FieldSpec("transaction_id", "string"),
            FieldSpec("reported_at", "timestamp_micros"),
            FieldSpec("reason", "string"),
            FieldSpec("created_at", "timestamp_micros"),
        ),
    ),
)


ENTITY_BY_TOPIC = {spec.topic: spec for spec in ENTITY_SPECS}

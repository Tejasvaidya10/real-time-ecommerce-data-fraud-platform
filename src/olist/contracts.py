from __future__ import annotations

import uuid
from datetime import UTC, datetime


REQUIRED_FILES = (
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
)

ORDER_STATUS_MAP = {
    "approved": "PAYMENT_PENDING",
    "canceled": "CANCELLED",
    "created": "CREATED",
    "delivered": "DELIVERED",
    "invoiced": "PAYMENT_PENDING",
    "processing": "PAYMENT_PENDING",
    "shipped": "SHIPPED",
    "unavailable": "FAILED",
}


def deterministic_uuid(entity: str, source_key: str) -> uuid.UUID:
    """Map a source key to a stable UUID without storing a lookup table."""
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"real-time-ecommerce-fraud-platform/olist/{entity}/{source_key}",
    )


def listing_key(product_id: str, seller_id: str) -> str:
    """The OLTP product table represents a seller-specific product listing."""
    return f"{product_id}:{seller_id}"


def map_order_status(source_status: str) -> str:
    try:
        return ORDER_STATUS_MAP[source_status.strip().lower()]
    except KeyError as exc:
        raise ValueError(f"Unsupported Olist order status: {source_status!r}") from exc


def map_payment_status(source_order_status: str) -> str:
    return "DECLINED" if source_order_status.strip().lower() in {"canceled", "unavailable"} else "AUTHORIZED"


def normalize_payment_type(source_payment_type: str) -> str:
    value = source_payment_type.strip().upper().replace(" ", "_")
    return value or "UNKNOWN"


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip())
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)

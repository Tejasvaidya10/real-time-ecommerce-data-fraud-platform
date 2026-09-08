from __future__ import annotations

import csv
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Iterator

import psycopg

from src.olist.contracts import (
    REQUIRED_FILES,
    deterministic_uuid,
    listing_key,
    map_order_status,
    map_payment_status,
    normalize_payment_type,
    parse_timestamp,
)


ZERO = Decimal("0.00")


@dataclass(frozen=True)
class OrderSource:
    order_id: str
    customer_id: str
    status: str
    purchased_at: datetime
    approved_at: datetime | None


def rows(path: Path) -> Iterator[dict[str, str]]:
    # utf-8-sig accepts both regular UTF-8 and the BOM found in some Kaggle ZIPs.
    with path.open(newline="", encoding="utf-8-sig") as handle:
        yield from csv.DictReader(handle)


def chunks(values: list, size: int) -> Iterator[list]:
    for start in range(0, len(values), size):
        yield values[start:start + size]


def execute_in_chunks(cur: psycopg.Cursor, statement: str, values: list[tuple], size: int = 1000) -> None:
    for batch in chunks(values, size):
        cur.executemany(statement, batch)


def require_dataset(root: Path) -> None:
    missing = [name for name in REQUIRED_FILES if not (root / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing Olist files under {root}: {', '.join(missing)}")


def load_sources(root: Path, order_limit: int) -> dict:
    customer_lookup: dict[str, tuple[str, str]] = {}
    unique_customer_state: dict[str, str] = {}
    for row in rows(root / "olist_customers_dataset.csv"):
        customer_lookup[row["customer_id"]] = (row["customer_unique_id"], row["customer_state"])
        unique_customer_state.setdefault(row["customer_unique_id"], row["customer_state"])

    selected_orders: list[OrderSource] = []
    order_customer: dict[str, str] = {}
    first_purchase: dict[str, datetime] = {}
    for row in rows(root / "olist_orders_dataset.csv"):
        purchased_at = parse_timestamp(row["order_purchase_timestamp"])
        order_id = row["order_id"]
        customer_id = row["customer_id"]
        order_customer[order_id] = customer_id
        unique_id = customer_lookup[customer_id][0]
        if unique_id not in first_purchase or purchased_at < first_purchase[unique_id]:
            first_purchase[unique_id] = purchased_at
        if order_limit == 0 or len(selected_orders) < order_limit:
            approved = row["order_approved_at"].strip()
            selected_orders.append(
                OrderSource(
                    order_id=order_id,
                    customer_id=customer_id,
                    status=row["order_status"],
                    purchased_at=purchased_at,
                    approved_at=parse_timestamp(approved) if approved else None,
                )
            )
    selected_orders.sort(key=lambda order: (order.purchased_at, order.order_id))
    selected_ids = {order.order_id for order in selected_orders}

    all_payment_totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
    selected_payments: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows(root / "olist_order_payments_dataset.csv"):
        value = Decimal(row["payment_value"])
        all_payment_totals[row["order_id"]] += value
        if row["order_id"] in selected_ids:
            selected_payments[row["order_id"]].append(row)

    customer_totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
    customer_order_counts: dict[str, int] = defaultdict(int)
    for order_id, total in all_payment_totals.items():
        customer_id = order_customer.get(order_id)
        if customer_id is None:
            continue
        unique_id = customer_lookup[customer_id][0]
        customer_totals[unique_id] += total
        customer_order_counts[unique_id] += 1

    selected_items: dict[str, list[dict[str, str]]] = defaultdict(list)
    product_ids: set[str] = set()
    seller_ids: set[str] = set()
    for row in rows(root / "olist_order_items_dataset.csv"):
        if row["order_id"] in selected_ids:
            selected_items[row["order_id"]].append(row)
            product_ids.add(row["product_id"])
            seller_ids.add(row["seller_id"])

    translations = {
        row["product_category_name"]: row["product_category_name_english"]
        for row in rows(root / "product_category_name_translation.csv")
    }
    products = {}
    for row in rows(root / "olist_products_dataset.csv"):
        if row["product_id"] in product_ids:
            source_category = row["product_category_name"] or "unknown"
            products[row["product_id"]] = translations.get(source_category, source_category)

    sellers = {}
    for row in rows(root / "olist_sellers_dataset.csv"):
        if row["seller_id"] in seller_ids:
            sellers[row["seller_id"]] = row["seller_state"]

    return {
        "customers": customer_lookup,
        "unique_customer_state": unique_customer_state,
        "first_purchase": first_purchase,
        "customer_totals": customer_totals,
        "customer_order_counts": customer_order_counts,
        "orders": selected_orders,
        "payments": selected_payments,
        "payment_totals": all_payment_totals,
        "items": selected_items,
        "products": products,
        "sellers": sellers,
    }


def seed_dimensions(conn: psycopg.Connection, source: dict) -> None:
    selected_unique_ids = {
        source["customers"][order.customer_id][0] for order in source["orders"]
    }
    customer_values = []
    for unique_id in sorted(selected_unique_ids):
        order_count = source["customer_order_counts"].get(unique_id, 0)
        average = (
            source["customer_totals"][unique_id] / order_count
            if order_count else ZERO
        ).quantize(Decimal("0.01"))
        customer_values.append(
            (
                deterministic_uuid("customer", unique_id),
                source["unique_customer_state"].get(unique_id, "UNKNOWN"),
                source["first_purchase"][unique_id] - timedelta(days=30),
                average,
                f"olist-trusted-{unique_id}",
            )
        )

    listing_prices: dict[str, list[Decimal]] = defaultdict(list)
    listing_meta: dict[str, tuple[str, str]] = {}
    for order_items in source["items"].values():
        for item in order_items:
            key = listing_key(item["product_id"], item["seller_id"])
            listing_prices[key].append(Decimal(item["price"]))
            listing_meta[key] = (item["product_id"], item["seller_id"])

    seller_values = [
        (deterministic_uuid("seller", seller_id), region or "UNKNOWN", "LOW")
        for seller_id, region in sorted(source["sellers"].items())
    ]
    product_values = []
    inventory_values = []
    for key, prices in sorted(listing_prices.items()):
        product_id, seller_id = listing_meta[key]
        listing_id = deterministic_uuid("listing", key)
        unit_price = (sum(prices, ZERO) / len(prices)).quantize(Decimal("0.01"))
        product_values.append(
            (
                listing_id,
                deterministic_uuid("seller", seller_id),
                source["products"].get(product_id, "unknown"),
                unit_price,
            )
        )
        inventory_values.append((listing_id, 1_000_000))

    with conn.cursor() as cur:
        execute_in_chunks(
            cur,
            """INSERT INTO commerce.customers
               (customer_id, home_region, account_created_at, average_order_amount, trusted_device_id)
               VALUES (%s, %s, %s, %s, %s) ON CONFLICT (customer_id) DO NOTHING""",
            customer_values,
        )
        execute_in_chunks(
            cur,
            """INSERT INTO commerce.sellers (seller_id, seller_region, risk_tier)
               VALUES (%s, %s, %s) ON CONFLICT (seller_id) DO NOTHING""",
            seller_values,
        )
        execute_in_chunks(
            cur,
            """INSERT INTO commerce.products (product_id, seller_id, category, unit_price)
               VALUES (%s, %s, %s, %s) ON CONFLICT (product_id) DO NOTHING""",
            product_values,
        )
        execute_in_chunks(
            cur,
            """INSERT INTO commerce.inventory (product_id, available_quantity)
               VALUES (%s, %s) ON CONFLICT (product_id) DO NOTHING""",
            inventory_values,
        )
    conn.commit()
    print(
        f"dimensions customers={len(customer_values)} sellers={len(seller_values)} "
        f"listings={len(product_values)}",
        flush=True,
    )


def replay_orders(conn: psycopg.Connection, source: dict, batch_size: int) -> tuple[int, int]:
    payment_count = 0
    skipped_zero_payments = 0
    for batch_number, orders in enumerate(chunks(source["orders"], batch_size), start=1):
        order_values = []
        item_values = []
        payment_values = []
        for order in orders:
            unique_id, region = source["customers"][order.customer_id]
            order_uuid = deterministic_uuid("order", order.order_id)
            raw_items = sorted(
                source["items"].get(order.order_id, []),
                key=lambda item: int(item["order_item_id"]),
            )
            item_total = sum(
                (Decimal(item["price"]) + Decimal(item["freight_value"]) for item in raw_items),
                ZERO,
            )
            order_total = source["payment_totals"].get(order.order_id, item_total)
            order_values.append(
                (
                    order_uuid,
                    deterministic_uuid("customer", unique_id),
                    map_order_status(order.status),
                    order_total.quantize(Decimal("0.01")),
                    region or "UNKNOWN",
                    order.purchased_at,
                )
            )

            grouped_items: dict[str, tuple[int, Decimal]] = {}
            for item in raw_items:
                key = listing_key(item["product_id"], item["seller_id"])
                quantity, total_price = grouped_items.get(key, (0, ZERO))
                grouped_items[key] = (quantity + 1, total_price + Decimal(item["price"]))
            for key, (quantity, total_price) in grouped_items.items():
                item_values.append(
                    (
                        order_uuid,
                        deterministic_uuid("listing", key),
                        quantity,
                        (total_price / quantity).quantize(Decimal("0.01")),
                    )
                )

            if not raw_items:
                continue
            primary_seller = raw_items[0]["seller_id"]
            average = (
                source["customer_totals"][unique_id]
                / max(1, source["customer_order_counts"].get(unique_id, 1))
            ).quantize(Decimal("0.01"))
            account_created_at = source["first_purchase"][unique_id] - timedelta(days=30)
            event_time = order.approved_at or order.purchased_at
            account_age_days = max(0, (event_time - account_created_at).days)
            for payment in source["payments"].get(order.order_id, []):
                amount = Decimal(payment["payment_value"]).quantize(Decimal("0.01"))
                if amount <= ZERO:
                    skipped_zero_payments += 1
                    continue
                payment_key = f"{order.order_id}:{payment['payment_sequential']}"
                payment_values.append(
                    (
                        deterministic_uuid("transaction", payment_key),
                        deterministic_uuid("payment-event", payment_key),
                        order_uuid,
                        deterministic_uuid("customer", unique_id),
                        deterministic_uuid("seller", primary_seller),
                        event_time,
                        amount,
                        normalize_payment_type(payment["payment_type"]),
                        f"olist-trusted-{unique_id}",
                        region or "UNKNOWN",
                        region or "UNKNOWN",
                        region or "UNKNOWN",
                        account_age_days,
                        average,
                        map_payment_status(order.status),
                    )
                )

        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO commerce.orders
                   (order_id, customer_id, status, total_amount, shipping_region, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (order_id) DO NOTHING""",
                order_values,
            )
            cur.executemany(
                """INSERT INTO commerce.order_items (order_id, product_id, quantity, unit_price)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (order_id, product_id) DO NOTHING""",
                item_values,
            )
            cur.executemany(
                """INSERT INTO commerce.payments
                   (transaction_id, event_id, order_id, customer_id, seller_id, event_time,
                    amount, currency, payment_type, device_id, is_new_device, ip_region,
                    home_region, shipping_region, account_age_days, login_failures,
                    attempt_number, customer_average_amount, status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, 'BRL', %s, %s, false, %s,
                           %s, %s, %s, 0, 1, %s, %s)
                   ON CONFLICT (transaction_id) DO NOTHING""",
                payment_values,
            )
        conn.commit()
        payment_count += len(payment_values)
        print(
            f"replayed_orders={min(batch_number * batch_size, len(source['orders']))}/"
            f"{len(source['orders'])} payments_seen={payment_count}",
            flush=True,
        )
    return payment_count, skipped_zero_payments


def main() -> None:
    dataset_root = Path(os.environ.get("OLIST_DATA_DIR", "/opt/project/data/olist/archive"))
    order_limit = int(os.environ.get("OLIST_ORDER_LIMIT", "10000"))
    batch_size = int(os.environ.get("OLIST_BATCH_SIZE", "500"))
    if order_limit < 0:
        raise ValueError("OLIST_ORDER_LIMIT must be zero (all orders) or positive")
    if batch_size <= 0:
        raise ValueError("OLIST_BATCH_SIZE must be positive")
    require_dataset(dataset_root)
    print(f"loading Olist dataset from {dataset_root} order_limit={order_limit or 'all'}", flush=True)
    source = load_sources(dataset_root, order_limit)

    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://commerce:commerce_dev_only@localhost:5432/commerce",
    )
    with psycopg.connect(database_url) as conn:
        seed_dimensions(conn, source)
        payment_count, skipped = replay_orders(conn, source, batch_size)
    print(
        f"complete orders={len(source['orders'])} payments_seen={payment_count} "
        f"zero_value_payments_skipped={skipped}",
        flush=True,
    )


if __name__ == "__main__":
    main()

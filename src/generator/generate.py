from __future__ import annotations

import os
import random
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg


REGIONS = ("US-NY", "US-CA", "US-TX", "US-FL", "US-WA")
CATEGORIES = ("electronics", "home", "books", "sports", "beauty")


@dataclass(frozen=True)
class Customer:
    customer_id: uuid.UUID
    home_region: str
    average_amount: Decimal
    trusted_device: str
    account_created_at: datetime


@dataclass(frozen=True)
class Product:
    product_id: uuid.UUID
    seller_id: uuid.UUID
    unit_price: Decimal


def deterministic_uuid(namespace: str, value: int, run_id: str = "reference") -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"ecommerce-fraud-platform/{run_id}/{namespace}/{value}")


def seed_reference_data(conn: psycopg.Connection, rng: random.Random) -> tuple[list[Customer], list[Product]]:
    now = datetime.now(UTC)
    customers: list[Customer] = []
    products: list[Product] = []

    with conn.cursor() as cur:
        for index in range(100):
            customer = Customer(
                customer_id=deterministic_uuid("customer", index),
                home_region=rng.choice(REGIONS),
                average_amount=Decimal(str(round(rng.uniform(25, 180), 2))),
                trusted_device=f"device-trusted-{index:04d}",
                account_created_at=now - timedelta(days=rng.randint(30, 1500)),
            )
            customers.append(customer)
            cur.execute(
                """
                INSERT INTO commerce.customers
                    (customer_id, home_region, account_created_at, average_order_amount, trusted_device_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    home_region = EXCLUDED.home_region,
                    average_order_amount = EXCLUDED.average_order_amount,
                    trusted_device_id = EXCLUDED.trusted_device_id,
                    updated_at = now()
                """,
                (customer.customer_id, customer.home_region, customer.account_created_at,
                 customer.average_amount, customer.trusted_device),
            )

        for seller_index in range(15):
            seller_id = deterministic_uuid("seller", seller_index)
            cur.execute(
                """
                INSERT INTO commerce.sellers (seller_id, seller_region, risk_tier)
                VALUES (%s, %s, %s)
                ON CONFLICT (seller_id) DO NOTHING
                """,
                (seller_id, rng.choice(REGIONS), rng.choices(("LOW", "MEDIUM", "HIGH"), (80, 15, 5))[0]),
            )
            for offset in range(4):
                product_index = seller_index * 4 + offset
                product = Product(
                    product_id=deterministic_uuid("product", product_index),
                    seller_id=seller_id,
                    unit_price=Decimal(str(round(rng.uniform(8, 350), 2))),
                )
                products.append(product)
                cur.execute(
                    """
                    INSERT INTO commerce.products (product_id, seller_id, category, unit_price)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (product_id) DO NOTHING
                    """,
                    (product.product_id, product.seller_id, rng.choice(CATEGORIES), product.unit_price),
                )
                cur.execute(
                    """
                    INSERT INTO commerce.inventory (product_id, available_quantity)
                    VALUES (%s, 10000)
                    ON CONFLICT (product_id) DO NOTHING
                    """,
                    (product.product_id,),
                )
    conn.commit()
    return customers, products


def generate_workload(conn: psycopg.Connection, customers: list[Customer], products: list[Product],
                      event_count: int, events_per_second: float, rng: random.Random,
                      run_id: str) -> None:
    pending_labels: list[tuple[int, uuid.UUID, str]] = []
    interval = 0 if events_per_second <= 0 else 1.0 / events_per_second

    for sequence in range(event_count):
        customer = rng.choice(customers)
        product = rng.choice(products)
        quantity = rng.randint(1, 3)
        normal_amount = max(float(product.unit_price * quantity), float(customer.average_amount) * rng.uniform(0.5, 1.5))
        scenario = rng.choices(
            ("LEGITIMATE", "HIGH_AMOUNT", "ACCOUNT_TAKEOVER", "RETRY_ABUSE"),
            weights=(990, 3, 5, 2),
            k=1,
        )[0]

        amount = round(normal_amount, 2)
        device_id = customer.trusted_device
        is_new_device = False
        ip_region = customer.home_region
        shipping_region = customer.home_region
        login_failures = 0
        attempt_number = 1

        if scenario == "HIGH_AMOUNT":
            amount = round(max(300.0, float(customer.average_amount) * 4.5), 2)
        elif scenario == "ACCOUNT_TAKEOVER":
            amount = round(max(350.0, float(customer.average_amount) * 5.0), 2)
            device_id = f"device-untrusted-{sequence:08d}"
            is_new_device = True
            ip_region = rng.choice(tuple(region for region in REGIONS if region != customer.home_region))
            shipping_region = ip_region
            login_failures = rng.randint(3, 8)
        elif scenario == "RETRY_ABUSE":
            device_id = f"device-retry-{sequence % 7}"
            is_new_device = True
            attempt_number = rng.randint(4, 8)

        order_id = deterministic_uuid("order", sequence, run_id)
        transaction_id = deterministic_uuid("transaction", sequence, run_id)
        event_id = deterministic_uuid("payment-event", sequence, run_id)
        event_time = datetime.now(UTC)
        account_age_days = max(0, (event_time - customer.account_created_at).days)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO commerce.orders
                    (order_id, customer_id, status, total_amount, shipping_region, created_at)
                VALUES (%s, %s, 'CREATED', %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
                """,
                (order_id, customer.customer_id, amount, shipping_region, event_time),
            )
            cur.execute(
                """
                INSERT INTO commerce.order_items (order_id, product_id, quantity, unit_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id, product_id) DO NOTHING
                """,
                (order_id, product.product_id, quantity, product.unit_price),
            )
            cur.execute(
                """
                INSERT INTO commerce.payments
                    (transaction_id, event_id, order_id, customer_id, seller_id, event_time,
                     amount, currency, payment_type, device_id, is_new_device, ip_region,
                     home_region, shipping_region, account_age_days, login_failures,
                     attempt_number, customer_average_amount, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'USD', 'CARD', %s, %s, %s, %s, %s, %s, %s, %s, %s, 'AUTHORIZED')
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                (transaction_id, event_id, order_id, customer.customer_id, product.seller_id,
                 event_time, amount, device_id, is_new_device, ip_region, customer.home_region,
                 shipping_region, account_age_days, login_failures, attempt_number,
                 customer.average_amount),
            )
            cur.execute(
                "UPDATE commerce.orders SET status = 'PAID', updated_at = now() WHERE order_id = %s",
                (order_id,),
            )
            cur.execute(
                """
                UPDATE commerce.inventory
                SET available_quantity = available_quantity - %s, updated_at = now()
                WHERE product_id = %s AND available_quantity >= %s
                """,
                (quantity, product.product_id, quantity),
            )

            due = [label for label in pending_labels if label[0] <= sequence]
            pending_labels = [label for label in pending_labels if label[0] > sequence]
            for _, labeled_transaction_id, reason in due:
                cur.execute(
                    """
                    INSERT INTO commerce.chargebacks (chargeback_id, transaction_id, reported_at, reason)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                    """,
                    (uuid.uuid4(), labeled_transaction_id, event_time, reason),
                )

        conn.commit()
        if scenario != "LEGITIMATE":
            pending_labels.append((sequence + rng.randint(20, 60), transaction_id, scenario))
        if interval:
            time.sleep(interval)

        if (sequence + 1) % max(1, min(1000, event_count // 10 or 1)) == 0:
            print(f"generated={sequence + 1}/{event_count}", flush=True)

    # Labels remain a separate, later database change even when the short demo
    # finishes before their simulated due sequence is reached.
    with conn.cursor() as cur:
        for delay, labeled_transaction_id, reason in pending_labels:
            cur.execute(
                """
                INSERT INTO commerce.chargebacks (chargeback_id, transaction_id, reported_at, reason)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                (
                    uuid.uuid4(),
                    labeled_transaction_id,
                    datetime.now(UTC) + timedelta(seconds=max(1, delay - event_count)),
                    reason,
                ),
            )
    conn.commit()


def main() -> None:
    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://commerce:commerce_dev_only@localhost:5432/commerce",
    )
    seed = int(os.environ.get("GENERATOR_SEED", "42"))
    event_count = int(os.environ.get("GENERATOR_EVENTS", "1000"))
    events_per_second = float(os.environ.get("GENERATOR_RATE", "25"))
    run_id = os.environ.get("GENERATOR_RUN_ID") or datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
    rng = random.Random(seed)

    with psycopg.connect(database_url) as conn:
        customers, products = seed_reference_data(conn, rng)
        generate_workload(conn, customers, products, event_count, events_per_second, rng, run_id)


if __name__ == "__main__":
    main()

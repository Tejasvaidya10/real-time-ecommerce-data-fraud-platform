CREATE SCHEMA IF NOT EXISTS commerce;
SET search_path TO commerce;

CREATE TABLE customers (
    customer_id UUID PRIMARY KEY,
    home_region TEXT NOT NULL,
    account_created_at TIMESTAMPTZ NOT NULL,
    average_order_amount NUMERIC(12,2) NOT NULL CHECK (average_order_amount >= 0),
    trusted_device_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE sellers (
    seller_id UUID PRIMARY KEY,
    seller_region TEXT NOT NULL,
    risk_tier TEXT NOT NULL CHECK (risk_tier IN ('LOW', 'MEDIUM', 'HIGH')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id UUID PRIMARY KEY,
    seller_id UUID NOT NULL REFERENCES sellers(seller_id),
    category TEXT NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory (
    product_id UUID PRIMARY KEY REFERENCES products(product_id),
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    status TEXT NOT NULL CHECK (status IN ('CREATED', 'PAYMENT_PENDING', 'PAID', 'FAILED', 'CANCELLED', 'SHIPPED', 'DELIVERED')),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    shipping_region TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id UUID NOT NULL REFERENCES orders(order_id),
    product_id UUID NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price > 0),
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE payments (
    transaction_id UUID PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    order_id UUID NOT NULL REFERENCES orders(order_id),
    customer_id UUID NOT NULL REFERENCES customers(customer_id),
    seller_id UUID NOT NULL REFERENCES sellers(seller_id),
    event_time TIMESTAMPTZ NOT NULL,
    amount NUMERIC(12,2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    payment_type TEXT NOT NULL,
    device_id TEXT NOT NULL,
    is_new_device BOOLEAN NOT NULL,
    ip_region TEXT NOT NULL,
    home_region TEXT NOT NULL,
    shipping_region TEXT NOT NULL,
    account_age_days INTEGER NOT NULL CHECK (account_age_days >= 0),
    login_failures INTEGER NOT NULL CHECK (login_failures >= 0),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    customer_average_amount NUMERIC(12,2) NOT NULL CHECK (customer_average_amount >= 0),
    status TEXT NOT NULL CHECK (status IN ('AUTHORIZED', 'DECLINED', 'PENDING')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX payments_customer_time_idx ON payments(customer_id, event_time);
CREATE INDEX payments_device_time_idx ON payments(device_id, event_time);

CREATE TABLE chargebacks (
    chargeback_id UUID PRIMARY KEY,
    transaction_id UUID UNIQUE NOT NULL REFERENCES payments(transaction_id),
    reported_at TIMESTAMPTZ NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

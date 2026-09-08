# Olist historical replay design

## Purpose

The replay adds real e-commerce behavior to the same change-data-capture path as
the synthetic fraud workload. It writes bounded batches to PostgreSQL; Debezium
captures those commits, Kafka retains and partitions the events, and the existing
Spark jobs produce Bronze lineage, Silver current state, explainable payment
decisions, and Gold analytics.

The default is 10,000 orders so the demo stays comfortable on a 16 GB MacBook Air.
`ORDERS=0` selects the complete 99,441-order dataset. Source timestamps are retained,
but commits happen as quickly as the local stack accepts them; this is an accelerated
historical replay rather than wall-clock simulation.

## Mapping decisions

| Olist source | Local target | Reason |
|---|---|---|
| `customer_unique_id` | `customers.customer_id` | Represents a person across Olist order-specific customer IDs. |
| `(product_id, seller_id)` | `products.product_id` | The operational schema assigns one seller per product, so the pair is a seller listing. |
| Olist order status | constrained local order status | Preserves meaning while satisfying the OLTP contract. |
| payment row | `payments` | Retains split tenders as separate transactions with BRL currency. |
| first seller on an order | payment seller | The local payment contract requires one seller; the full multi-seller relationship remains in order items and Databricks. |
| first purchase minus 30 days | account creation | Olist does not publish account creation; the deterministic assumption supplies the fraud contract's required nonnegative account age. |
| deterministic trusted device | payment device | Olist has no device data; the replay makes that absence neutral rather than fabricating suspicious devices. |

`payment_sequential` is a split-tender sequence, not a retry count, so every imported
payment uses `attempt_number=1`. Reviews and geolocation are not forced into the local
OLTP schema; the Databricks notebooks use both directly.

## Fraud interpretation

Olist has no verified fraud or chargeback outcome. The replay never creates false
chargebacks. Local rule decisions can still identify amount anomalies, but the
Databricks `payment_risk_signals` output is labeled
`HEURISTIC_PROXY_NOT_CONFIRMED_FRAUD`. Synthetic delayed chargebacks remain the only
ground truth used to test the fraud system.

## Idempotence

Every source business key is converted to a stable UUIDv5 within an Olist-specific
namespace. Inserts use primary-key conflict handling, so rerunning an identical or
overlapping slice produces no duplicate business records and no duplicate CDC inserts.

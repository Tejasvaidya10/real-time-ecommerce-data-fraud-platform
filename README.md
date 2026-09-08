# Real-Time E-Commerce Data & Fraud Detection Platform

A zero-cost, production-pattern e-commerce CDC lakehouse with real-time payment fraud detection. It is tuned for a 16 GB Apple Silicon Mac and runs locally with Docker Desktop.

## What the platform currently contains

- PostgreSQL operational tables for customers, sellers, products, inventory, orders, payments, and delayed chargebacks.
- Debezium PostgreSQL CDC through Kafka Connect.
- Apache Kafka in single-node KRaft mode with internal and host listeners.
- A deterministic Python workload generator with labeled fraud scenarios.
- Spark Structured Streaming Bronze ingestion with Kafka lineage metadata.
- Idempotent Silver Delta merges for customers, orders, payments, and chargebacks.
- Data-quality quarantine keyed by Kafka partition and offset.
- A first explainable fraud scorer that writes decisions to Delta Lake.
- Transactional Gold analytics for daily business KPIs, daily fraud KPIs,
  seller performance, and customer 360 reporting.
- A responsive local command-center dashboard for commerce and fraud operations.
- An idempotent Olist historical replay through PostgreSQL, Debezium, Kafka, and Spark.
- Databricks Free Edition notebooks for Olist Bronze, Silver, Gold, risk analytics, and dashboard datasets.
- Avro contracts and unit tests for the scoring rules.

The local topology intentionally uses one Kafka broker, replication factor one, and Spark `local[2]`. `docs/architecture.md` explains the production mapping.

## Prerequisites

- Docker Desktop, running with at least 8 GB assigned to its Linux VM.
- `make`, `curl`, and Python 3.10+ on the host.
- Internet access for the first image and Maven dependency downloads only.

## Quick start

```bash
make init
make validate
make up
make register-connector
make generate EVENTS=1000 RATE=25
make bronze
make silver
make fraud
make dashboard
make verify-pipeline
make status
```

To add real Olist history after placing the nine CSV files in
`data/olist/archive`, run a safe 10,000-order slice:

```bash
make olist-replay ORDERS=10000 BATCH_SIZE=500
make gold
make verify-pipeline
```

Use `ORDERS=0` only when you want all 99,441 source orders. Replaying the same
slice is safe: namespaced UUIDs and database conflict handling prevent duplicate
business records. The CSV files remain ignored by Git.

The Spark services are continuous streaming jobs. Inspect them with:

```bash
docker compose -f infra/compose.yaml --env-file .env --profile processing logs -f spark-bronze spark-silver spark-fraud
```

Local Delta output is written under `data/lakehouse`, checkpoints under `data/checkpoints`,
and PostgreSQL/Kafka runtime files under `data/runtime`. All three paths are ignored by Git.

## Gold analytics

`make gold` performs a deterministic batch refresh from the current Silver snapshot. Each
table is replaced with a new Delta transaction, which makes scheduled reruns and backfills
safe without accumulating duplicate aggregates.

| Table | Grain | Primary use |
|---|---|---|
| `daily_business_kpis` | Date and currency | Payment volume, customer reach, GMV, and chargebacks |
| `daily_fraud_kpis` | Date and rules version | Decisions, flagged value, confirmed fraud, and rates |
| `seller_performance` | Seller | GMV, customer reach, fraud exposure, and chargeback rate |
| `customer_360` | Customer | Lifetime value, activity, fraud history, and risk segment |

Metric definitions and reconciliation rules are documented in `docs/gold-metrics.md`.

## Local dashboard

`make dashboard` refreshes Gold, writes an aggregate-only dashboard snapshot, and starts
the interface at `http://127.0.0.1:8080`. The service binds only to localhost. Its JSON
feed contains no customer, payment, transaction, or seller identifiers and remains under
the Git-ignored `data/exports` directory.

The first viewport combines GMV, payment volume, current customers, active sellers,
flagged rate, chargeback rate, fraud decisions, and value at risk. Seller rankings are
anonymized; customer data is presented only as aggregated risk segments.

## Useful endpoints

| Service | Address |
|---|---|
| PostgreSQL | `localhost:5432` |
| Kafka | `localhost:9092` |
| Kafka Connect REST | `http://localhost:8083` |
| Commerce Risk dashboard | `http://127.0.0.1:8080` |

## Fraud scenarios in milestone one

- High amount relative to the customer's baseline.
- New device.
- IP/home-region mismatch.
- Repeated login failures.
- Repeated payment attempts.
- A deterministic combination representing account takeover.

Labels are emitted separately as chargeback rows after a configurable number of later events. Payment records never contain the ground-truth fraud label.

## Real data and Databricks

The Olist dataset comes from the public [Brazilian E-Commerce Public Dataset by
Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). It contains
real commerce behavior but no verified fraud labels. The local replay therefore
feeds Olist payments through the same explainable decision path without inventing
chargebacks. The Databricks Gold notebook creates a clearly labeled heuristic risk
proxy for exploratory triage, while the synthetic stream supplies delayed, known
fraud labels for evaluation.

Run the notebooks in `databricks/notebooks` using `databricks/README.md`. They use
a Unity Catalog volume and managed Delta tables, so no paid storage account or
always-on cluster is needed for this portfolio path. Raw CSV data is uploaded
manually and is never committed.

## Repository map

```text
infra/                 Docker Compose and database initialization
schemas/               Avro event contracts
src/generator/         Deterministic operational workload generator
src/olist/             Idempotent real-data historical replay
spark/jobs/            Structured Streaming jobs
dashboard/             Local aggregate-only portfolio dashboard
databricks/             Free Edition medallion notebooks and runbook
scripts/               Local lifecycle helpers
tests/                 Dependency-free unit tests
docs/                  Architecture decisions and roadmap
data/                   Git-ignored local lakehouse/checkpoint storage
```

## Current boundary

This is a runnable Bronze/Silver/Gold platform with real-time CDC, explainable
fraud decisions, real Olist historical replay, reconciled analytics, a local
dashboard, and a Databricks medallion implementation. Stateful velocity features,
SCD Type 2 dimensions, orchestration, failure drills, and million-event benchmarks
remain explicitly tracked in `docs/roadmap.md`.

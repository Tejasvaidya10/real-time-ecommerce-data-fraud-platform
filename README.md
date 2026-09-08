# Real-Time E-Commerce Data & Fraud Detection Platform

A zero-cost, production-pattern e-commerce CDC lakehouse with real-time payment fraud detection. It is tuned for a 16 GB Apple Silicon Mac and runs locally with Docker Desktop.

## What the first milestone contains

- PostgreSQL operational tables for customers, sellers, products, inventory, orders, payments, and delayed chargebacks.
- Debezium PostgreSQL CDC through Kafka Connect.
- Apache Kafka in single-node KRaft mode with internal and host listeners.
- A deterministic Python workload generator with labeled fraud scenarios.
- Spark Structured Streaming Bronze ingestion with Kafka lineage metadata.
- A first explainable fraud scorer that writes decisions to Delta Lake.
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
make fraud
make status
```

The Spark services are continuous streaming jobs. Inspect them with:

```bash
docker compose -f infra/compose.yaml --env-file .env --profile processing logs -f spark-bronze spark-fraud
```

Local Delta output is written under `data/lakehouse` and checkpoints under `data/checkpoints`.

## Useful endpoints

| Service | Address |
|---|---|
| PostgreSQL | `localhost:5432` |
| Kafka | `localhost:9092` |
| Kafka Connect REST | `http://localhost:8083` |

## Fraud scenarios in milestone one

- High amount relative to the customer's baseline.
- New device.
- IP/home-region mismatch.
- Repeated login failures.
- Repeated payment attempts.
- A deterministic combination representing account takeover.

Labels are emitted separately as chargeback rows after a configurable number of later events. Payment records never contain the ground-truth fraud label.

## Data

Milestone one is entirely synthetic and requires no download. The next milestone will add an importer for the free Olist Brazilian e-commerce dataset. Raw third-party data will remain outside Git.

## Repository map

```text
infra/                 Docker Compose and database initialization
schemas/               Avro event contracts
src/generator/         Deterministic operational workload generator
spark/jobs/            Structured Streaming jobs
scripts/               Local lifecycle helpers
tests/                 Dependency-free unit tests
docs/                  Architecture decisions and roadmap
data/                   Git-ignored local lakehouse/checkpoint storage
```

## Current boundary

This is the first runnable vertical slice. Stateful velocity features, Silver CDC merges, Gold tables, Olist ingestion, Grafana, Airflow, failure drills, benchmarks, and Databricks notebooks are deliberately tracked in `docs/roadmap.md` rather than hidden behind placeholder code.

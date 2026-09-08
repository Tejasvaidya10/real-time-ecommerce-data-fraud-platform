# Delivery roadmap

## Milestone 1 — runnable vertical slice

- [x] PostgreSQL commerce schema
- [x] Kafka KRaft and topic initialization
- [x] Debezium CDC configuration
- [x] Deterministic workload and fraud injection
- [x] Bronze Delta ingestion
- [x] Explainable fraud decisions
- [x] Unit tests and Compose validation

## Milestone 2 — contracts and curated lakehouse

- [ ] Local schema registry and compatibility checks in CI
- [x] Silver CDC parser and idempotent Delta merges
- [ ] Customer/product SCD Type 2 history
- [x] Dead-letter and quarantine handling
- [x] Gold retail and fraud aggregates

## Milestone 3 — stateful fraud

- [ ] One-, five-, and thirty-minute velocity features
- [ ] Distinct-device and device-sharing features
- [ ] Watermarks and deliberately late events
- [ ] Decisions published back to Kafka
- [ ] Chargeback-to-feature training table

## Milestone 4 — operations

- [ ] Prometheus and Grafana profile
- [ ] Airflow reconciliation/backfill profile
- [ ] Data-quality assertions
- [ ] Failure and replay drills
- [ ] One-million and five-million-event benchmarks

## Milestone 5 — portfolio surface

- [x] Local commerce and fraud command-center dashboard
- [ ] Olist importer and accelerated replay
- [ ] Databricks Free Edition notebooks
- [ ] Databricks SQL dashboard
- [ ] Architecture and lineage diagrams
- [ ] Demo script and resume-ready benchmark report

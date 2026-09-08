# Architecture decisions

## Local-first execution

The project must cost nothing and run on a 16 GB M4 MacBook Air. The local topology therefore uses one Kafka broker, one Kafka Connect worker, PostgreSQL, and independent Spark `local[2]` jobs. Compose profiles keep optional services stopped.

Production mapping:

| Local component | Production equivalent |
|---|---|
| One Kafka broker, RF=1 | Three or more brokers across failure domains, RF=3 |
| Spark `local[2]` | Autoscaled Spark cluster with separate driver and executors |
| Project-local bind-mounted storage | Replicated object storage |
| Plaintext listeners | TLS, authentication, authorization, and secret management |
| One Connect worker | Distributed Kafka Connect worker group |
| Local checkpoints | Durable object-storage checkpoints |

## CDC instead of polling

Debezium reads PostgreSQL logical replication and records inserts, updates, and deletes. This preserves ordering information and avoids timestamp polling gaps.

## Real history as accelerated CDC replay

The Olist importer maps public source keys to stable, namespaced UUIDs and commits
orders in configurable batches. This exercises the same PostgreSQL-to-Debezium-to-
Kafka path as live transactions while finishing historical backfills quickly.
Conflict-safe inserts make overlapping replays idempotent. Seller-specific product
listings preserve the operational schema's one-seller-per-product invariant.

## Raw Debezium envelopes in Bronze

Bronze keeps `before`, `after`, operation, source metadata, Kafka partition, and offset. Flattening happens later so source history remains auditable and reprocessable.

## Idempotent current-state tables in Silver

Silver reads the Bronze Delta stream and parses both schema-wrapped and schemaless Debezium envelopes. Each micro-batch keeps only the latest source LSN per primary key, then applies inserts, updates, and deletes with Delta `MERGE`. Kafka coordinates and PostgreSQL LSNs remain on every curated row for replay audits. Invalid records are merged into a quarantine table by a deterministic Kafka-coordinate identifier.

## Labels are separate from payments

Payments do not contain a fraud label. Chargebacks arrive later, reflecting delayed ground truth and preventing accidental label leakage.

## Transactional serving tables in Gold

Gold joins current payments to explainable fraud decisions and delayed chargebacks, then produces daily business KPIs, daily fraud KPIs, seller performance, and customer 360 tables. Gold is a bounded snapshot refresh rather than another always-on stream because its inputs include mutable current-state tables and cross-domain joins. Delta overwrite commits each table atomically; reconciliation gates prevent an incomplete Silver or fraud snapshot from being published as a successful refresh. In production, an orchestrator would run this job after upstream freshness checks.

## Aggregate-only dashboard boundary

The Gold refresh atomically replaces a small JSON dashboard snapshot after all Delta tables are published. The export contains daily aggregates, anonymized seller ranks, aggregated customer risk segments, freshness, and quality counts; it excludes entity and transaction identifiers. A 64 MB static web container reads the export through a read-only mount and binds only to `127.0.0.1`. This keeps the portfolio interface free, lightweight, and private while preserving a clear production mapping to a BI serving API or governed warehouse.

## Explainable rules before ML

The first decision engine uses deterministic rules. This isolates pipeline correctness from model quality and produces reason codes suitable for operational review. A time-split ML model is a later enhancement.

## Idempotency boundary

Kafka and external Delta/PostgreSQL sinks do not automatically form one distributed transaction. Stable event and decision identifiers, checkpoints, deduplication, and `MERGE`-based sinks are used to produce effectively-once business outcomes.

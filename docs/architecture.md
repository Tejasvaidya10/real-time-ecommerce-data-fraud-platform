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

## Raw Debezium envelopes in Bronze

Bronze keeps `before`, `after`, operation, source metadata, Kafka partition, and offset. Flattening happens later so source history remains auditable and reprocessable.

## Idempotent current-state tables in Silver

Silver reads the Bronze Delta stream and parses both schema-wrapped and schemaless Debezium envelopes. Each micro-batch keeps only the latest source LSN per primary key, then applies inserts, updates, and deletes with Delta `MERGE`. Kafka coordinates and PostgreSQL LSNs remain on every curated row for replay audits. Invalid records are merged into a quarantine table by a deterministic Kafka-coordinate identifier.

## Labels are separate from payments

Payments do not contain a fraud label. Chargebacks arrive later, reflecting delayed ground truth and preventing accidental label leakage.

## Explainable rules before ML

The first decision engine uses deterministic rules. This isolates pipeline correctness from model quality and produces reason codes suitable for operational review. A time-split ML model is a later enhancement.

## Idempotency boundary

Kafka and external Delta/PostgreSQL sinks do not automatically form one distributed transaction. Stable event and decision identifiers, checkpoints, deduplication, and `MERGE`-based sinks are used to produce effectively-once business outcomes.

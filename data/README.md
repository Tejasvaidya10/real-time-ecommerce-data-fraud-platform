# Local data

This directory is the boundary for generated and third-party data:

- `olist/`: user-downloaded Olist CSV files; never committed.
- `lakehouse/`: local Bronze, Silver, and Gold Delta tables; never committed.
- `checkpoints/`: Spark streaming checkpoints; never committed.
- `runtime/`: project-local PostgreSQL and Kafka storage; never committed.
- `exports/`: sanitized local dashboard exports; never committed.

The repository contains generators and contracts, not redistributed source datasets.

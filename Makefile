SHELL := /bin/bash
COMPOSE := docker compose -f infra/compose.yaml --env-file .env
SPARK_PACKAGES := io.delta:delta-spark_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1
EVENTS ?= 1000
RATE ?= 25

.PHONY: init validate test up down status topics register-connector generate bronze fraud verify-pipeline logs clean-data

init:
	@test -f .env || cp .env.example .env
	@mkdir -p data/lakehouse data/checkpoints data/runtime/postgres data/runtime/kafka data/olist data/exports

validate: init
	$(COMPOSE) config --quiet
	python3 -m unittest discover -s tests -v

test:
	python3 -m unittest discover -s tests -v

up: init
	$(COMPOSE) up -d postgres kafka kafka-init connect

down:
	$(COMPOSE) --profile processing down

status:
	$(COMPOSE) --profile processing ps

topics:
	$(COMPOSE) run --rm kafka-init

register-connector:
	./scripts/register_connector.sh

generate:
	$(COMPOSE) --profile tools run --rm --build -e GENERATOR_EVENTS=$(EVENTS) -e GENERATOR_RATE=$(RATE) generator

bronze:
	$(COMPOSE) --profile processing up -d spark-bronze

fraud:
	$(COMPOSE) --profile processing up -d spark-fraud

verify-pipeline:
	$(COMPOSE) --profile processing exec -T spark-fraud /opt/spark/bin/spark-submit --master local[1] --driver-memory 512m --packages $(SPARK_PACKAGES) /opt/project/spark/jobs/verify_pipeline.py

logs:
	$(COMPOSE) --profile processing logs -f --tail=100

clean-data:
	@echo "Refusing to delete data automatically. Remove data/lakehouse and data/checkpoints manually if intended."

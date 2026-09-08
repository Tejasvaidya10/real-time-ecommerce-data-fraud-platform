SHELL := /bin/bash
COMPOSE := docker compose -f infra/compose.yaml --env-file .env
EVENTS ?= 1000
RATE ?= 25

.PHONY: init validate test up down status topics register-connector generate bronze fraud logs clean-data

init:
	@test -f .env || cp .env.example .env
	@mkdir -p data/lakehouse data/checkpoints data/olist data/exports

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
	$(COMPOSE) --profile tools run --rm -e GENERATOR_EVENTS=$(EVENTS) -e GENERATOR_RATE=$(RATE) generator

bronze:
	$(COMPOSE) --profile processing up -d spark-bronze

fraud:
	$(COMPOSE) --profile processing up -d spark-fraud

logs:
	$(COMPOSE) --profile processing logs -f --tail=100

clean-data:
	@echo "Refusing to delete data automatically. Remove data/lakehouse and data/checkpoints manually if intended."

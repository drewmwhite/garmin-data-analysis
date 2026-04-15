.DEFAULT_GOAL := help

SHELL := /bin/bash

PYTHON ?= $(if $(wildcard venv/bin/python),venv/bin/python,python3)
UVICORN ?= $(PYTHON) -m uvicorn
PORT ?= 8200
TABLE ?= all
LIMIT ?=
BUCKET ?=
PREFIX ?= garmin
DATASET ?= all
CACHE_DIR ?=
TEST ?=

.PHONY: help
help: ## Show available commands and common variables.
	@printf "\nRunbook\n\n"
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_.-]+:.*## / {printf "  %-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf "\nVariables:\n"
	@printf "  PORT=8200           API port for make api\n"
	@printf "  TABLE=...           Table for build-table, default: all\n"
	@printf "  LIMIT=...           Optional row/file limit for build/upload targets\n"
	@printf "  BUCKET=...          S3 bucket for build-s3 and upload-s3\n"
	@printf "  PREFIX=garmin       S3 prefix for build-s3 and upload-s3\n"
	@printf "  DATASET=...         Dataset for upload-s3-dataset\n"
	@printf "  CACHE_DIR=...       Cached Strava JSON dir for build-strava-cache\n"
	@printf "  TEST=...            Test module/path for test-one\n\n"

.PHONY: install
install: ## Install backend Python dependencies into the local venv.
	$(PYTHON) -m pip install -r backend/requirements.txt

.PHONY: api
api: ## Start the FastAPI backend with reload enabled.
	PYTHONPATH=backend/src $(UVICORN) api.app:app --reload --port $(PORT)

.PHONY: frontend-serve
frontend-serve: ## Serve the static frontend locally on port 8080.
	$(PYTHON) -m http.server 8080 --directory frontend

.PHONY: extract-debug
extract-debug: ## Run the standalone extraction runner for local parser checks.
	PYTHONPATH=backend/src $(PYTHON) backend/src/extraction/runner.py

.PHONY: build
build: ## Rebuild the full garmin.duckdb database.
	$(PYTHON) db/build.py

.PHONY: build-table
build-table: ## Rebuild a specific table: make build-table TABLE=strava LIMIT=100
	$(PYTHON) db/build.py --table $(TABLE) $(if $(LIMIT),--limit $(LIMIT),)

.PHONY: build-strava
build-strava: ## Incrementally fetch Strava data from the API and rebuild Strava tables.
	$(PYTHON) db/build.py --table strava $(if $(LIMIT),--limit $(LIMIT),)

.PHONY: build-strava-30d
build-strava-30d: ## Fetch and upsert only the last 30 days of Strava activities and laps.
	$(PYTHON) db/build.py --table strava --strava-recent-days 30 $(if $(LIMIT),--limit $(LIMIT),)

.PHONY: build-strava-cache
build-strava-cache: ## Rebuild Strava tables from saved JSON: make build-strava-cache CACHE_DIR=logs/strava_api/<run>
	@if [ -z "$(CACHE_DIR)" ]; then echo "CACHE_DIR is required"; exit 2; fi
	$(PYTHON) db/build.py --table strava --strava-cache-dir "$(CACHE_DIR)" $(if $(LIMIT),--limit $(LIMIT),)

.PHONY: build-s3
build-s3: ## Rebuild the full DB and upload tables to S3 in one pass.
	@if [ -z "$(BUCKET)" ]; then echo "BUCKET is required"; exit 2; fi
	$(PYTHON) db/build.py --bucket "$(BUCKET)" --prefix "$(PREFIX)" $(if $(LIMIT),--limit $(LIMIT),)

.PHONY: upload-s3
upload-s3: ## Upload all datasets to S3 as partitioned Parquet.
	@if [ -z "$(BUCKET)" ]; then echo "BUCKET is required"; exit 2; fi
	$(PYTHON) backend/scripts/upload_to_s3.py --bucket "$(BUCKET)" --prefix "$(PREFIX)"

.PHONY: upload-s3-dataset
upload-s3-dataset: ## Upload one dataset to S3: make upload-s3-dataset BUCKET=... DATASET=activity-sessions
	@if [ -z "$(BUCKET)" ]; then echo "BUCKET is required"; exit 2; fi
	$(PYTHON) backend/scripts/upload_to_s3.py --bucket "$(BUCKET)" --prefix "$(PREFIX)" --dataset "$(DATASET)" $(if $(LIMIT),--limit $(LIMIT),)

.PHONY: test
test: ## Run the full backend test suite.
	$(PYTHON) -m unittest discover -s backend/tests -v

.PHONY: test-api
test-api: ## Run API endpoint tests.
	$(PYTHON) -m unittest backend.tests.test_api -v

.PHONY: test-db
test-db: ## Run database build tests.
	$(PYTHON) -m unittest backend.tests.test_db_build -v

.PHONY: test-strava
test-strava: ## Run Strava extractor and Strava DB build tests.
	$(PYTHON) -m unittest backend.tests.test_db_build backend.tests.test_strava_extractor -v

.PHONY: test-training-plan
test-training-plan: ## Run training plan service tests.
	$(PYTHON) -m unittest backend.tests.test_training_plan_service -v

.PHONY: test-one
test-one: ## Run one test module or test case: make test-one TEST=backend.tests.test_duckdb_service
	@if [ -z "$(TEST)" ]; then echo "TEST is required"; exit 2; fi
	$(PYTHON) -m unittest "$(TEST)" -v

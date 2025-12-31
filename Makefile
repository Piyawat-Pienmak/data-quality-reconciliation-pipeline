SHELL := /bin/bash
PY := .venv/bin/python

.PHONY: help up down reset-db venv run-good run-bad psql

help:
	@echo "Targets:"
	@echo "  make up         - start Postgres (docker compose)"
	@echo "  make down       - stop containers"
	@echo "  make reset-db   - stop containers and delete volumes (FULL RESET)"
	@echo "  make venv       - create venv and install dependencies"
	@echo "  make run-good   - run pipeline with passing dataset (DATA_DIR=data_good)"
	@echo "  make run-bad    - run pipeline with failing dataset (DATA_DIR=data) (expected FAIL)"
	@echo "  make psql       - open psql inside the Postgres container"

up:
	docker compose up -d

down:
	docker compose down

reset-db:
	docker compose down -v

venv:
	python3 -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

run-good:
	DATA_DIR=data_good $(PY) -m src.run_pipeline

run-bad:
	@set +e; \
	DATA_DIR=data $(PY) -m src.run_pipeline; \
	code=$$?; \
	echo "pipeline exit code: $$code"; \
	if [ $$code -eq 1 ]; then \
		echo "Expected failure occurred (strict reconciliation gate)."; \
		exit 0; \
	fi; \
	echo "Unexpected result. Expected exit code 1 but got $$code"; \
	exit 1

psql:
	docker exec -it dq_pg psql -U postgres -d dq

PY := python3

.PHONY: help install install-dev lint format test qa v1 v3 runlog

help:
	@echo "make install      -> install runtime deps"
	@echo "make install-dev  -> install runtime + dev deps"
	@echo "make lint         -> ruff check"
	@echo "make format       -> ruff format"
	@echo "make test         -> pytest"
	@echo "make qa           -> lint + format check + tests"
	@echo "make v1           -> run V1 pipeline"
	@echo "make v3           -> run V3 pipeline"
	@echo "make runlog       -> write a reproducibility run log"

install:
	@# This installs only the runtime dependencies needed to run the pipeline scripts.
	$(PY) -m pip install -r requirements.txt

install-dev:
	@# This installs runtime + QA tooling (tests + linting + formatting).
	$(PY) -m pip install -r requirements.txt -r requirements-dev.txt

lint:
	@# This checks code quality (errors, unused imports, bad patterns).
	ruff check .

format:
	@# This auto-formats code to a consistent style.
	ruff format .

test:
	@# This runs the unit tests.
	pytest

qa:
	@# This is the “CI locally” command: fail fast if anything is off.
	ruff check .
	ruff format --check .
	pytest

v1:
	@# This runs the V1 pipeline (events → clean → publish).
	$(PY) src/bq_smoke_test.py
	$(PY) src/extract_events_daily.py
	$(PY) src/clean_events_daily.py
	$(PY) src/publish_tableau_table.py

v3:
	@# This runs the V3 pipeline (risk table + next-day forecast publish).
	$(PY) src/create_country_risk_daily_table.py
	$(PY) src/publish_risk_forecasts.py

runlog:
	@# This writes a run log so refreshes are auditable.
	$(PY) src/write_run_log.py

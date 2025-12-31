# Data Quality + Reconciliation Pipeline

![CI](https://github.com/Piyawat-Pienmak/data-quality-reconciliation-pipeline/actions/workflows/ci.yml/badge.svg)

End-to-end **data trust gate** using **Postgres + Python** that:
- loads raw data → cleans/standardizes it → runs **data quality tests**
- runs **row-level reconciliation** (missing/extra keys)
- runs **metric reconciliation** (daily totals/counts)
- **fails fast** with explainable terminal output when numbers drift

This repo demonstrates real-world issues like **missing keys**, **late/duplicate records**, and **mismatched totals**, and records each run in an **audit table**.

---

## Architecture

**Layers**
- `raw.*`       : as-received data (messy allowed)
- `staging.*`   : cleaned + typed tables (enforced constraints)
- `marts.*`     : business outputs (example: daily revenue fact)
- `quality.*`   : tests + reconciliation results (auditable via `run_id`)

**Flow**
1. Load CSV → `raw`
2. Transform + dedup → `staging`
3. Run tests → `quality.test_results`
4. Reconcile:
   - row-level mismatches → `quality.recon_row_mismatches`
   - metric summaries → `quality.recon_summary`
5. Build mart → `marts.fact_revenue_daily`
6. Record run audit → `quality.pipeline_runs`

---

## Quickstart

### Prerequisites
- Docker Desktop
- Python 3.10+ (recommended 3.11)

### Start Postgres
```bash
docker compose up -d
```

### Create venv + install deps
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Tip: you can use the included `Makefile` (e.g., `make up`, `make venv`, `make run-good`, `make run-bad`).

### Run pipeline (GOOD dataset - should PASS)
```bash
DATA_DIR=data_good python -m src.run_pipeline
```

### Run pipeline (BAD dataset - should FAIL)
```bash
DATA_DIR=data python -m src.run_pipeline
```

Expected behavior (strict mode):
- Any **row mismatch** (e.g., payment references missing order) causes failure.
- Any **metric drift** (daily count/amount mismatch) causes failure.
- The terminal prints mismatch samples + failing metrics for fast debugging.

---

## Case study: catching a revenue drift

**Symptom:** Daily paid revenue drifted by **+16.7%** on **2025-12-29**.  
**Gate behavior:** Pipeline failed fast (strict reconciliation).  
**Root cause (row-level):** `payment_id=P9004` referenced missing `order_id=O9999`.  
**Impact (metric-level):** payments=350 vs orders=300 (delta=+50).  
**Evidence:** mismatches in `quality.recon_row_mismatches`, metrics in `quality.recon_summary`, run audit in `quality.pipeline_runs`.

---

## Debugging

### Open Postgres
```bash
docker exec -it dq_pg psql -U postgres -d dq
```

### Inspect reconciliation + tests
```sql
-- filter by run_id printed in terminal if needed
SELECT * FROM quality.recon_row_mismatches ORDER BY run_ts DESC LIMIT 50;
SELECT * FROM quality.recon_summary ORDER BY metric_date, metric_name;
SELECT * FROM quality.test_results ORDER BY run_ts DESC;
```

---

## Run history (audit)

Each run writes a record to `quality.pipeline_runs` including:
`run_id`, dataset (`data_dir`), status, test/recon flags, mismatch counts, timestamps (and git SHA in CI).

```sql
SELECT run_id, data_dir, status, tests_ok, recon_ok, mismatch_count, suppressed_mismatch_count, failing_metric_count, started_ts, finished_ts
FROM quality.pipeline_runs
ORDER BY started_ts DESC
LIMIT 10;
```

---

## Exception workflow (controlled suppressions)

Known mismatches can be suppressed with a ticket + expiry in `quality.recon_exceptions`.
Suppressed mismatches are still logged (auditable), but do not fail the run.

```sql
INSERT INTO quality.recon_exceptions (mismatch_type, key, expires_at, ticket_id, reason)
VALUES ('payment_order_missing', 'P9004', now() + interval '7 days', 'JIRA-1234', 'Known issue')
ON CONFLICT (mismatch_type, key)
DO UPDATE SET expires_at = EXCLUDED.expires_at,
              ticket_id = EXCLUDED.ticket_id,
              reason = EXCLUDED.reason;
```

---

## Datasets

- `data/`      : intentionally broken cases (missing order, drift) for demo + “expected fail” CI job
- `data_good/` : consistent dataset for passing runs and CI

---

## CI

GitHub Actions runs:
- `DATA_DIR=data_good` must pass
- `DATA_DIR=data` must fail (expected-fail job proves the strict gate works)

---

## What this repo proves (interview talking points)
- Understanding of **grain**, dedup, and join-explosion prevention
- **Idempotent** pipeline runs (safe re-run)
- Data quality checks that write auditable results
- Reconciliation at both **row-level** and **metric-level**
- Fail-fast behavior suitable for CI/CD quality gates
- Run audit table for production-style observability
- Controlled suppression workflow (ticket + expiry)

---

## Next upgrades (optional roadmap)
- dbt migration for transforms + tests
- Prefect/Airflow scheduling + retries

---

## License
MIT

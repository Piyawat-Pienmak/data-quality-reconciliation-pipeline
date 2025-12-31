# Data Quality + Reconciliation Pipeline

End-to-end **data trust gate** using **Postgres + Python** that:
- loads raw data → cleans/standardizes it → runs **data quality tests**
- runs **row-level reconciliation** (missing/extra keys)
- runs **metric reconciliation** (daily totals/counts)
- **fails fast** with explainable terminal output when numbers drift

This repo demonstrates real-world issues like **missing keys**, **late/duplicate records**, and **mismatched totals**.

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

---

## Quickstart

### Prerequisites
- Docker Desktop
- Python 3.10+ (recommended 3.11)

### Start Postgres
```bash
docker compose up -d

from src.db import exec_sql

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS quality;
CREATE SCHEMA IF NOT EXISTS marts;

-- RAW (as received)
CREATE TABLE IF NOT EXISTS raw.orders_raw (
  order_id     TEXT,
  customer_id  TEXT,
  order_ts     TIMESTAMPTZ,
  status       TEXT,
  amount       TEXT  -- keep as TEXT first (raw reality)
);

CREATE TABLE IF NOT EXISTS raw.payments_raw (
  payment_id   TEXT,
  order_id     TEXT,
  paid_ts      TIMESTAMPTZ,
  status       TEXT,
  amount       TEXT
);

-- STAGING (clean types + minimal rules)
CREATE TABLE IF NOT EXISTS staging.orders_clean (
  order_id     TEXT PRIMARY KEY,
  customer_id  TEXT NOT NULL,
  order_ts     TIMESTAMPTZ NOT NULL,
  status       TEXT NOT NULL,
  amount       NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.payments_clean (
  payment_id   TEXT PRIMARY KEY,
  order_id     TEXT NOT NULL,
  paid_ts      TIMESTAMPTZ NOT NULL,
  status       TEXT NOT NULL,
  amount       NUMERIC NOT NULL
);

-- QUALITY outputs
CREATE TABLE IF NOT EXISTS quality.test_results (
  run_id       TEXT,
  test_name    TEXT,
  passed       BOOLEAN,
  details      TEXT,
  run_ts       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS quality.recon_summary (
  run_id         TEXT,
  metric_date    DATE,
  metric_name    TEXT,
  system_a_value NUMERIC,
  system_b_value NUMERIC,
  delta          NUMERIC,
  delta_pct      NUMERIC,
  passed         BOOLEAN,
  run_ts         TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS quality.recon_row_mismatches (
  run_id      TEXT,
  mismatch_type TEXT,
  key         TEXT,
  details     TEXT,
  run_ts      TIMESTAMPTZ DEFAULT now()
);

-- MARTS
CREATE TABLE IF NOT EXISTS marts.fact_revenue_daily (
  day         DATE PRIMARY KEY,
  paid_amount NUMERIC NOT NULL,
  paid_count  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS quality.pipeline_runs (
  run_id              TEXT PRIMARY KEY,
  data_dir            TEXT NOT NULL,
  git_sha             TEXT,
  started_ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_ts         TIMESTAMPTZ,
  status              TEXT,        -- SUCCESS | FAIL | ERROR
  tests_ok            BOOLEAN,
  recon_ok            BOOLEAN,
  mismatch_count      INTEGER,
  failing_metric_count INTEGER,
  error_message       TEXT
);

"""

def create_all(conn):
    # Split by semicolon; execute each non-empty statement
    for stmt in [s.strip() for s in DDL.split(";")]:
        if stmt:
            exec_sql(conn, stmt + ";")

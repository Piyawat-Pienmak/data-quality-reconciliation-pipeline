import os
import uuid
import sys
from src.db import get_conn, exec_sql
from src.ddl import create_all
from src.load_raw import load_raw
from src.transform import transform
from src.quality import run_tests
from src.reconcile import reconcile
from src.marts import build_marts

def _insert_run_start(conn, run_id: str, data_dir: str, git_sha: str | None):
    exec_sql(conn, """
      INSERT INTO quality.pipeline_runs (run_id, data_dir, git_sha, status)
      VALUES (%s, %s, %s, %s);
    """, (run_id, data_dir, git_sha, "RUNNING"))

def _update_run_finish(
    conn,
    run_id: str,
    status: str,
    tests_ok: bool | None,
    recon_ok: bool | None,
    mismatch_count: int | None,
    failing_metric_count: int | None,
    suppressed_mismatch_count: int | None,
    error_message: str | None,
):
    exec_sql(conn, """
      UPDATE quality.pipeline_runs
      SET finished_ts = now(),
          status = %s,
          tests_ok = %s,
          recon_ok = %s,
          mismatch_count = %s,
          failing_metric_count = %s,
          suppressed_mismatch_count = %s,
          error_message = %s
      WHERE run_id = %s;
    """, (
        status,
        tests_ok,
        recon_ok,
        mismatch_count,
        failing_metric_count,
        suppressed_mismatch_count,
        error_message,
        run_id,
    ))


def _get_recon_counts(conn, run_id: str) -> tuple[int, int, int]:
    with conn.cursor() as cur:
        cur.execute("""
          SELECT
            SUM(CASE WHEN suppressed = false THEN 1 ELSE 0 END) AS active_cnt,
            SUM(CASE WHEN suppressed = true  THEN 1 ELSE 0 END) AS suppressed_cnt
          FROM quality.recon_row_mismatches
          WHERE run_id = %s;
        """, (run_id,))
        active_cnt, suppressed_cnt = cur.fetchone()
        active_cnt = active_cnt or 0
        suppressed_cnt = suppressed_cnt or 0

        cur.execute("""
          SELECT COUNT(*)
          FROM quality.recon_summary
          WHERE run_id = %s AND passed = false;
        """, (run_id,))
        failing_metric_count = cur.fetchone()[0]

    return active_cnt, failing_metric_count, suppressed_cnt

def main():
    run_id = str(uuid.uuid4())
    data_dir = os.getenv("DATA_DIR", "data")
    git_sha = os.getenv("GITHUB_SHA")  # present in GitHub Actions, usually None locally

    conn = get_conn()
    try:
        conn.autocommit = False

        # Ensure schemas/tables exist
        create_all(conn)
        conn.commit()

        # Record run start (commit it so it survives later rollbacks)
        _insert_run_start(conn, run_id, data_dir, git_sha)
        conn.commit()

        # Main pipeline work (transaction)
        load_raw(conn)
        transform(conn)

        tests_ok = run_tests(conn, run_id)
        recon_ok = reconcile(conn, run_id, tolerance_pct=0)

        build_marts(conn)

        mismatch_count, failing_metric_count, suppressed_mismatch_count = _get_recon_counts(conn, run_id)

        status = "SUCCESS" if (tests_ok and recon_ok) else "FAIL"
        _update_run_finish(
            conn,
            run_id,
            status=status,
            tests_ok=tests_ok,
            recon_ok=recon_ok,
            mismatch_count=mismatch_count,
            failing_metric_count=failing_metric_count,
            suppressed_mismatch_count=suppressed_mismatch_count,
            error_message=None,
        )

        conn.commit()

        print(f"[run] run_id={run_id} status={status} data_dir={data_dir} mismatches={mismatch_count} suppressed={suppressed_mismatch_count} failing_metrics={failing_metric_count}")

        if status != "SUCCESS":
            sys.exit(1)

        sys.exit(0)

    except Exception as e:
        # Roll back whatever failed in the main transaction
        try:
            conn.rollback()
        except Exception:
            pass

        # Try to record ERROR status (best effort)
        try:
            _update_run_finish(
                conn,
                run_id,
                status="ERROR",
                tests_ok=None,
                recon_ok=None,
                mismatch_count=None,
                failing_metric_count=None,
                suppressed_mismatch_count=None,
                error_message=str(e),
            )
            conn.commit()
        except Exception:
            pass

        print("[ERROR]", e)
        sys.exit(2)

    finally:
        conn.close()

if __name__ == "__main__":
    main()

from decimal import Decimal
from src.db import exec_sql

def reconcile(conn, run_id: str, tolerance_pct: Decimal = Decimal("0.0")) -> bool:
    """
    Strict mode:
    - If any row mismatches exist => FAIL
    - Also check metric reconciliation (amount + count)
    - Print mismatch samples + failing metrics to terminal
    """

    # IMPORTANT: don't TRUNCATE here if you want history across runs
    # (run_id already isolates each run)

    # 1) Row-level mismatches: PAID payments referencing missing orders
    exec_sql(conn, """
      INSERT INTO quality.recon_row_mismatches (run_id, mismatch_type, key, details)
      SELECT %s, 'payment_order_missing', p.payment_id,
             'order_id=' || p.order_id || ', amount=' || p.amount::text
      FROM staging.payments_clean p
      LEFT JOIN staging.orders_clean o
        ON p.order_id = o.order_id
      WHERE o.order_id IS NULL
        AND p.status = 'PAID';
    """, (run_id,))

    # Count mismatches + print sample
    with conn.cursor() as cur:
        cur.execute("""
          SELECT COUNT(*)
          FROM quality.recon_row_mismatches
          WHERE run_id = %s;
        """, (run_id,))
        mismatch_count = cur.fetchone()[0]

    print(f"[recon] row_mismatches={mismatch_count}")

    if mismatch_count > 0:
        with conn.cursor() as cur:
            cur.execute("""
              SELECT mismatch_type, key, details
              FROM quality.recon_row_mismatches
              WHERE run_id = %s
              ORDER BY run_ts
              LIMIT 20;
            """, (run_id,))
            sample = cur.fetchall()

        print("[recon] mismatch sample (first 20):")
        for mt, k, d in sample:
            print(f"  - {mt}: {k} | {d}")

    # 2) Metric-level reconciliation: daily PAID totals (payments vs orders)
    with conn.cursor() as cur:
        cur.execute("""
          WITH a AS (
            SELECT paid_ts::date AS day, SUM(amount) AS paid_amount, COUNT(*) AS paid_count
            FROM staging.payments_clean
            WHERE status = 'PAID'
            GROUP BY 1
          ),
          b AS (
            SELECT order_ts::date AS day, SUM(amount) AS paid_amount, COUNT(*) AS paid_count
            FROM staging.orders_clean
            WHERE status = 'PAID'
            GROUP BY 1
          ),
          days AS (
            SELECT day FROM a
            UNION
            SELECT day FROM b
          )
          SELECT
            d.day,
            COALESCE(a.paid_amount, 0) AS a_amt,
            COALESCE(b.paid_amount, 0) AS b_amt,
            COALESCE(a.paid_count, 0) AS a_cnt,
            COALESCE(b.paid_count, 0) AS b_cnt
          FROM days d
          LEFT JOIN a ON a.day = d.day
          LEFT JOIN b ON b.day = d.day
          ORDER BY d.day;
        """)
        rows = cur.fetchall()

    passed_all = True
    failing_metrics = []

    for day, a_amt, b_amt, a_cnt, b_cnt in rows:
        # amount metric
        delta = a_amt - b_amt
        delta_pct = Decimal("0")
        if b_amt != 0:
            delta_pct = (Decimal(a_amt) - Decimal(b_amt)) / Decimal(b_amt)

        passed_amt = (abs(delta_pct) <= tolerance_pct)
        passed_all &= passed_amt

        exec_sql(conn, """
          INSERT INTO quality.recon_summary
            (run_id, metric_date, metric_name, system_a_value, system_b_value, delta, delta_pct, passed)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (run_id, day, "paid_amount_daily", a_amt, b_amt, delta, delta_pct, passed_amt))

        if not passed_amt:
            failing_metrics.append(
                f"paid_amount_daily {day}: payments={a_amt} orders={b_amt} delta={delta} delta_pct={delta_pct}"
            )

        # count metric (exact match)
        passed_cnt = (a_cnt == b_cnt)
        passed_all &= passed_cnt

        exec_sql(conn, """
          INSERT INTO quality.recon_summary
            (run_id, metric_date, metric_name, system_a_value, system_b_value, delta, delta_pct, passed)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (run_id, day, "paid_count_daily", a_cnt, b_cnt, (a_cnt - b_cnt), None, passed_cnt))

        if not passed_cnt:
            failing_metrics.append(
                f"paid_count_daily {day}: payments={a_cnt} orders={b_cnt} delta={(a_cnt - b_cnt)}"
            )

    if failing_metrics:
        print("[recon] failing metrics:")
        for line in failing_metrics:
            print("  -", line)

    # STRICT GATE: mismatches => fail
    if mismatch_count > 0:
        passed_all = False

    return passed_all

from src.db import exec_sql

ACCEPTED_ORDER_STATUS = ("PAID", "CANCELLED")
ACCEPTED_PAY_STATUS = ("PAID", "REFUND")

def _insert_test(conn, run_id: str, test_name: str, passed: bool, details: str):
    exec_sql(conn, """
      INSERT INTO quality.test_results (run_id, test_name, passed, details)
      VALUES (%s, %s, %s, %s);
    """, (run_id, test_name, passed, details))

def run_tests(conn, run_id: str) -> bool:
    """
    Returns True if all tests pass.
    """
    all_passed = True

    # 1) uniqueness: orders_clean.order_id
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT order_id) FROM staging.orders_clean;")
        total, distinct_ = cur.fetchone()
    passed = (total == distinct_)
    _insert_test(conn, run_id, "orders_clean_order_id_unique", passed, f"total={total}, distinct={distinct_}")
    all_passed &= passed

    # 2) accepted values: order status
    with conn.cursor() as cur:
        cur.execute("""
          SELECT COUNT(*)
          FROM staging.orders_clean
          WHERE status <> ALL(%s);
        """, (list(ACCEPTED_ORDER_STATUS),))
        bad = cur.fetchone()[0]
    passed = (bad == 0)
    _insert_test(conn, run_id, "orders_status_accepted", passed, f"bad_rows={bad}")
    all_passed &= passed

    # 3) accepted values: payment status
    with conn.cursor() as cur:
        cur.execute("""
          SELECT COUNT(*)
          FROM staging.payments_clean
          WHERE status <> ALL(%s);
        """, (list(ACCEPTED_PAY_STATUS),))
        bad = cur.fetchone()[0]
    passed = (bad == 0)
    _insert_test(conn, run_id, "payments_status_accepted", passed, f"bad_rows={bad}")
    all_passed &= passed

    # 4) not-null check: staging columns already constrained, but check anyway (example)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM staging.payments_clean WHERE order_id IS NULL OR order_id = '';")
        bad = cur.fetchone()[0]
    passed = (bad == 0)
    _insert_test(conn, run_id, "payments_order_id_not_null", passed, f"bad_rows={bad}")
    all_passed &= passed

    return all_passed

from src.db import exec_sql

def transform(conn):
    # Clear staging each run (simple MVP idempotency)
    exec_sql(conn, "TRUNCATE staging.orders_clean;")
    exec_sql(conn, "TRUNCATE staging.payments_clean;")

    # Orders: dedup by order_id (keep latest by order_ts), cast amount, fill missing amount as 0
    exec_sql(conn, """
    INSERT INTO staging.orders_clean (order_id, customer_id, order_ts, status, amount)
    SELECT order_id, customer_id, order_ts, UPPER(status) AS status,
           COALESCE(NULLIF(amount, '')::numeric, 0) AS amount
    FROM (
      SELECT
        order_id, customer_id, order_ts, status, amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_ts DESC) AS rn
      FROM raw.orders_raw
      WHERE order_id IS NOT NULL AND order_id <> ''
    ) t
    WHERE rn = 1;
    """)

    # Payments: cast amount, normalize status
    exec_sql(conn, """
    INSERT INTO staging.payments_clean (payment_id, order_id, paid_ts, status, amount)
    SELECT payment_id, order_id, paid_ts, UPPER(status) AS status,
           COALESCE(NULLIF(amount, '')::numeric, 0) AS amount
    FROM raw.payments_raw
    WHERE payment_id IS NOT NULL AND payment_id <> '';
    """)

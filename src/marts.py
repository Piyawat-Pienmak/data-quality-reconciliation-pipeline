from src.db import exec_sql

def build_marts(conn):
    exec_sql(conn, "TRUNCATE marts.fact_revenue_daily;")
    exec_sql(conn, """
      INSERT INTO marts.fact_revenue_daily (day, paid_amount, paid_count)
      SELECT paid_ts::date AS day,
             SUM(amount) AS paid_amount,
             COUNT(*) AS paid_count
      FROM staging.payments_clean
      WHERE status = 'PAID'
      GROUP BY 1
      ORDER BY 1;
    """)

import csv
import os
from pathlib import Path
from src.db import exec_sql

def truncate_raw(conn):
    exec_sql(conn, "TRUNCATE raw.orders_raw;")
    exec_sql(conn, "TRUNCATE raw.payments_raw;")

def load_csv_to_table(conn, csv_path: str, table: str, columns: list[str]):
    path = Path(csv_path)
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        col_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders});"

        rows = [tuple(r[c] for c in columns) for r in reader]

    with conn.cursor() as cur:
        cur.executemany(sql, rows)

def load_raw(conn):
    truncate_raw(conn)

    data_dir = os.getenv("DATA_DIR", "data")  # <-- default stays your current data/

    load_csv_to_table(
        conn,
        f"{data_dir}/orders.csv",
        "raw.orders_raw",
        ["order_id", "customer_id", "order_ts", "status", "amount"],
    )

    load_csv_to_table(
        conn,
        f"{data_dir}/payments.csv",
        "raw.payments_raw",
        ["payment_id", "order_id", "paid_ts", "status", "amount"],
    )

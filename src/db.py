import os
import psycopg
from dotenv import load_dotenv

load_dotenv()  # loads .env into environment variables

def get_conn():
    """
    Returns a psycopg connection.
    """
    url = os.environ["DATABASE_URL"]  # KeyError if missing (good: fail fast)
    return psycopg.connect(url)

def exec_sql(conn, sql: str, params=None):
    """
    Execute one SQL statement.
    params is optional; use it for safe parameterized queries.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)  # cur.execute uses %s placeholders (not f-string)

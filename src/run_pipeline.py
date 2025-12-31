import uuid
import sys
from src.db import get_conn
from src.ddl import create_all
from src.load_raw import load_raw
from src.transform import transform
from src.quality import run_tests
from src.reconcile import reconcile
from src.marts import build_marts

def main():
    run_id = str(uuid.uuid4())

    conn = get_conn()
    try:
        conn.autocommit = False  # we control commits

        create_all(conn)
        load_raw(conn)
        transform(conn)

        tests_ok = run_tests(conn, run_id)
        recon_ok = reconcile(conn, run_id, tolerance_pct=0)  # exact match for MVP

        build_marts(conn)

        conn.commit()

        if not tests_ok or not recon_ok:
            print(f"[FAIL] run_id={run_id} tests_ok={tests_ok} recon_ok={recon_ok}")
            sys.exit(1)

        print(f"[OK] run_id={run_id} pipeline succeeded")
        sys.exit(0)

    except Exception as e:
        conn.rollback()
        print("[ERROR]", e)
        sys.exit(2)
    finally:
        conn.close()

if __name__ == "__main__":
    main()

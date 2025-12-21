import sqlite3

def run_checks(db_path: str) -> dict:
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        fk = conn.execute("PRAGMA foreign_key_check;").fetchall()
        return {"fk_violations": len(fk)}
    finally:
        conn.close()

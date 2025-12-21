import sqlite3
from pathlib import Path

def migrate(db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        # Minimal migration table
        conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY)")
        conn.commit()
    finally:
        conn.close()

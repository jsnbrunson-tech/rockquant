import sqlite3
from pathlib import Path

def migrate(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.OperationalError:
            pass

        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()

        migrations_dir = Path(__file__).parent / "migrations"
        files = sorted(migrations_dir.glob("*.sql"))
        applied = set(r[0] for r in cur.execute("SELECT version FROM schema_migrations"))

        for mf in files:
            version = int(mf.stem.split("_")[0])
            if version in applied:
                continue
            sql = mf.read_text(encoding="utf-8")
            cur.executescript(sql)
            cur.execute("INSERT INTO schema_migrations (version) VALUES (?)", (version,))
            conn.commit()
    finally:
        conn.close()

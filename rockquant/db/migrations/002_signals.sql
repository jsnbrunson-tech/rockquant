-- 002_signals.sql: Signals table

CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_date TEXT NOT NULL,
  signal_type TEXT NOT NULL,
  signal_scope TEXT NOT NULL DEFAULT 'global',
  score REAL NOT NULL DEFAULT 0,
  evidence_count INTEGER NOT NULL DEFAULT 0,
  supporting_event_ids TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(signal_date, signal_type, signal_scope)
);

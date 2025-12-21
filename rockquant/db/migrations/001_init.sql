-- 001_init.sql: Initial schema for M3

CREATE TABLE IF NOT EXISTS source_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url TEXT NOT NULL UNIQUE,
  published_date TEXT,
  raw_html TEXT,
  fetched_at TEXT DEFAULT (datetime('now')),
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  canonical_event_key TEXT NOT NULL UNIQUE,
  event_date TEXT NOT NULL,
  title TEXT NOT NULL,
  url TEXT,
  event_subtype TEXT,
  event_type TEXT,
  source_doc_id INTEGER,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (source_doc_id) REFERENCES source_documents(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date);
CREATE INDEX IF NOT EXISTS idx_events_source ON events(source_doc_id);

from __future__ import annotations
import sqlite3
from collections import defaultdict

# Simple deterministic scoring for FAST-41 press releases (v1)
SUBTYPE_TO_SIGNAL = {
    "FAST41_ADDED": ("permitting_velocity", 3.0),
    "PERMITTING_APPROVAL": ("permitting_velocity", 3.0),
    "MILESTONE_UPDATE": ("permitting_milestone", 2.0),
    "PROGRESS_UPDATE": ("permitting_milestone", 2.0),
    "SITE_VISIT": ("policy_signal", 1.0),
    "FAST41_NEWS": ("policy_signal", 0.5),
}

def generate_fast41_signals(db_path: str) -> dict:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Determine which columns exist in signals table so we insert safely
    sig_cols = [r["name"] for r in cur.execute("PRAGMA table_info(signals)").fetchall()]
    required = ["signal_date","signal_type","signal_scope","score","evidence_count"]
    for c in required:
        if c not in sig_cols:
            con.close()
            return {"signals": 0, "reason": f"signals table missing column: {c}"}

    # Ensure source_entity_id exists in events (we added it manually earlier)
    evt_cols = [r["name"] for r in cur.execute("PRAGMA table_info(events)").fetchall()]
    if "source_entity_id" not in evt_cols:
        con.close()
        return {"signals": 0, "reason": "events.source_entity_id missing"}

    # Pull FAST41 events and aggregate per day per subtype
    rows = cur.execute("""
      SELECT event_date, event_subtype
      FROM events
      WHERE COALESCE(source_entity_id,'') = 'FAST41_COUN'
    """).fetchall()

    buckets = defaultdict(int)
    for r in rows:
        d = (r["event_date"] or "").strip()
        st = (r["event_subtype"] or "").strip()
        if not d or not st:
            continue
        buckets[(d, st)] += 1

    inserted = 0
    for (d, st), n in buckets.items():
        sig_type, base_score = SUBTYPE_TO_SIGNAL.get(st, ("policy_signal", 0.5))
        scope = "FAST41_COUN"

        # Dedup: do not insert if same (date,type,scope) already exists
        exists = cur.execute("""
          SELECT 1 FROM signals
          WHERE signal_date=? AND signal_type=? AND signal_scope=?
          LIMIT 1
        """, (d, sig_type, scope)).fetchone()
        if exists:
            continue

        cur.execute("""
          INSERT INTO signals (signal_date, signal_type, signal_scope, score, evidence_count)
          VALUES (?, ?, ?, ?, ?)
        """, (d, sig_type, scope, float(base_score), int(n)))
        inserted += 1

    con.commit()
    con.close()
    return {"signals": inserted}

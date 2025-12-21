import sqlite3
import json

SUBTYPE_TO_SIGNAL = {
    "DEAL_CLOSED_LOAN": ("funding_velocity", 3.0),
    "DEAL_CLOSED_LOAN_GUARANTEE": ("funding_velocity", 3.0),
    "DISBURSEMENT_APPROVED": ("funding_velocity", 2.0),
    "DEAL_RESTRUCTURED": ("risk_indicator", 1.0),
    "CONDITIONAL_COMMITMENT_TERMINATED": ("risk_indicator", -3.0),
    "REPAYMENT_RECEIVED": ("funding_velocity", 1.0),
    "DEAL_ANNOUNCED": ("opportunity_alert", 1.0),
    "CONDITIONAL_COMMITMENT_ANNOUNCED": ("opportunity_alert", 1.0),
    "CONDITIONAL_COMMITMENT_ISSUED": ("opportunity_alert", 2.0),
}

def generate_signals(db_path: str) -> dict:
    """Generate signals from events by aggregating subtypes."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Read all events with their date, subtype, and id
    events = cursor.execute("""
        SELECT id, event_date, event_subtype
        FROM events
        ORDER BY event_date
    """).fetchall()
    
    # Aggregate by (date, signal_type, scope)
    signals_map = {}  # key: (date, signal_type, scope) -> value: {score, event_ids}
    
    for event_id, event_date, event_subtype in events:
        if event_subtype not in SUBTYPE_TO_SIGNAL:
            continue
            
        signal_type, weight = SUBTYPE_TO_SIGNAL[event_subtype]
        key = (event_date, signal_type, "global")
        
        if key not in signals_map:
            signals_map[key] = {"score": 0.0, "event_ids": []}
        
        signals_map[key]["score"] += weight
        signals_map[key]["event_ids"].append(event_id)
    
    # UPSERT signals into database
    signals_count = 0
    for (signal_date, signal_type, signal_scope), data in signals_map.items():
        supporting_event_ids = json.dumps(data["event_ids"])
        evidence_count = len(data["event_ids"])
        score = data["score"]
        
        cursor.execute("""
            INSERT INTO signals (signal_date, signal_type, signal_scope, score, evidence_count, supporting_event_ids)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_date, signal_type, signal_scope)
            DO UPDATE SET 
                score = excluded.score,
                evidence_count = excluded.evidence_count,
                supporting_event_ids = excluded.supporting_event_ids,
                created_at = datetime('now')
        """, (signal_date, signal_type, signal_scope, score, evidence_count, supporting_event_ids))
        signals_count += 1
    
    conn.commit()
    conn.close()
    
    return {"signals": signals_count}

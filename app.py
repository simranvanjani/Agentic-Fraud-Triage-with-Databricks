"""
Live Fraud Queue - Databricks App
Analysts review yellow-flagged transactions; Block / Allow / Escalate.
"""
import os
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="Live Fraud Queue")

# Lakebase connection for triage table
def get_pg_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("PGHOST"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE"),
        user=os.environ.get("PGUSER"),
        password=os.environ.get("PGPASSWORD", ""),
        sslmode="require",
    )

class TriageAction(BaseModel):
    transaction_id: str
    action: str  # ALLOW, BLOCK, ESCALATE
    reviewed_by: Optional[str] = None
    notes: Optional[str] = None

@app.get("/api/queue")
def get_queue():
    """List yellow-flagged (e.g. CHALLENGE/MEDIUM) items for review."""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT transaction_id, user_id, risk_score, risk_level, automated_action, action_reason, review_status, created_at
            FROM real_time_fraud_triage
            WHERE review_status = 'PENDING' AND automated_action IN ('CHALLENGE', 'MONITOR')
            ORDER BY created_at DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [{"transaction_id": r[0], "user_id": r[1], "risk_score": float(r[2]), "risk_level": r[3], "automated_action": r[4], "action_reason": r[5], "review_status": r[6], "created_at": str(r[7])} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/review")
def post_review(body: TriageAction):
    """Analyst action: ALLOW / BLOCK / ESCALATE."""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE real_time_fraud_triage
            SET review_status = %s, reviewed_by = %s, review_notes = %s, review_timestamp = CURRENT_TIMESTAMP
            WHERE transaction_id = %s
        """, ("APPROVED" if body.action == "ALLOW" else "REJECTED" if body.action == "BLOCK" else "ESCALATED", body.reviewed_by or "analyst", body.notes, body.transaction_id))
        conn.commit()
        cur.close()
        conn.close()
        return {"ok": True, "transaction_id": body.transaction_id, "action": body.action}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Static frontend
frontend = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(frontend):
    @app.get("/")
    def index():
        return FileResponse(os.path.join(frontend, "index.html"))
    @app.get("/index.html")
    def index_html():
        return FileResponse(os.path.join(frontend, "index.html"))

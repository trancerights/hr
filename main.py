import os
import sqlite3
import json
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

app = FastAPI()

DB_PATH = os.environ.get("DB_PATH", "hr.db")
HR_API_TOKEN = os.environ.get("HR_API_TOKEN", "change-me")

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("""
    CREATE TABLE IF NOT EXISTS hrm_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bpm INTEGER,
        hrv_rmssd REAL,
        steps INTEGER,
        timestamp TEXT NOT NULL
    )
""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON hrm_data(timestamp)")
conn.commit()


class HrmInput(BaseModel):
    bpm: int
    hrv_rmssd: float | None = None
    steps: int | None = None
    timestamp: str | None = None


def compute_stress(rmssd: float | None) -> str:
    if rmssd is None:
        return "unknown"
    if rmssd > 50:
        return "low"
    if rmssd >= 30:
        return "median"
    return "high"


def get_response_data() -> dict:
    cur = conn.execute(
        "SELECT bpm, hrv_rmssd, steps, timestamp FROM hrm_data ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return {"bpm": None, "hrv_rmssd": None, "stress": "unknown", "steps": None, "timestamp": None, "stale": True}

    bpm, hrv_rmssd, steps, timestamp = row
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    stale = datetime.now(timezone.utc) - dt > timedelta(minutes=10)

    return {
        "bpm": bpm,
        "hrv_rmssd": hrv_rmssd,
        "stress": compute_stress(hrv_rmssd),
        "steps": steps,
        "timestamp": timestamp,
        "stale": stale,
    }


sse_clients = []
sse_lock = threading.Lock()


@app.post("/")
async def ingest(data: HrmInput, authorization: str = Header(None)):
    if not authorization or not authorization.startswith(f"Bearer {HR_API_TOKEN}"):
        raise HTTPException(status_code=401, detail="Invalid or missing token. Include 'Authorization: Bearer YOUR_TOKEN' header.")

    timestamp = data.timestamp or datetime.now(timezone.utc).isoformat()

    conn.execute(
        "INSERT INTO hrm_data (bpm, hrv_rmssd, steps, timestamp) VALUES (?, ?, ?, ?)",
        (data.bpm, data.hrv_rmssd, data.steps, timestamp)
    )
    conn.commit()

    response = get_response_data()

    with sse_lock:
        dead_clients = []
        for client in sse_clients:
            try:
                client.put_nowait(f"data: {json.dumps(response)}\n\n")
            except:
                dead_clients.append(client)
        for client in dead_clients:
            sse_clients.remove(client)

    return {"ok": True}


@app.get("/")
async def get_latest():
    return get_response_data()


@app.get("/range/{period}")
async def get_range(period: str):
    now = datetime.now(timezone.utc)
    if period == "day":
        start = now - timedelta(days=1)
    elif period == "week":
        start = now - timedelta(days=7)
    elif period == "month":
        start = now - timedelta(days=30)
    else:
        raise HTTPException(status_code=400, detail="Invalid period")

    cur = conn.execute(
        "SELECT bpm, hrv_rmssd, steps, timestamp FROM hrm_data WHERE timestamp >= ? ORDER BY id",
        (start.isoformat(),)
    )
    rows = cur.fetchall()
    return [
        {"bpm": r[0], "hrv_rmssd": r[1], "steps": r[2], "timestamp": r[3]}
        for r in rows
    ]


@app.get("/test")
async def test(authorization: str = Header(None)):
    return {
        "status": "ok",
        "token_valid": authorization == f"Bearer {HR_API_TOKEN}",
        "received": authorization,
        "expected": f"Bearer {HR_API_TOKEN}",
        "token_set": bool(HR_API_TOKEN and HR_API_TOKEN != "change-me")
    }


@app.get("/stream")
async def stream(request: Request):
    async def event_generator():
        queue = __import__("queue").Queue()
        with sse_lock:
            sse_clients.append(queue)

        try:
            while True:
                try:
                    data = queue.get(timeout=30)
                    yield f"event: hrm\n{data}"
                except:
                    pass

                if await request.is_disconnected():
                    break
        finally:
            with sse_lock:
                if queue in sse_clients:
                    sse_clients.remove(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

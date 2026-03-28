import os
import sqlite3
import json
import math
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
        hrv REAL,
        steps INTEGER,
        timestamp TEXT NOT NULL
    )
""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON hrm_data(timestamp)")

# Migrate old column name if needed
try:
    conn.execute("ALTER TABLE hrm_data RENAME COLUMN hrv_rmssd TO hrv")
except Exception:
    pass

conn.commit()


class HrmInput(BaseModel):
    bpm: int
    hrv: float | None = None
    steps: int | None = None
    timestamp: str | None = None


class AndroidHrmInput(BaseModel):
    device_id: str | None = None
    heart_rate: int
    rr_intervals: list[int] | None = None
    sensor_contact: bool | None = None
    battery_level: int | None = None
    recorded_at: int | None = None  # Unix timestamp in milliseconds


def compute_rmssd(rr_intervals: list[int]) -> float | None:
    if not rr_intervals or len(rr_intervals) < 2:
        return None
    diffs = [(rr_intervals[i + 1] - rr_intervals[i]) for i in range(len(rr_intervals) - 1)]
    squared_diffs = [d * d for d in diffs]
    mean_sq = sum(squared_diffs) / len(squared_diffs)
    return round(math.sqrt(mean_sq), 2)


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
        "SELECT bpm, hrv, steps, timestamp FROM hrm_data ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return {"bpm": None, "hrv": None, "steps": None, "timestamp": None, "stale": True}

    bpm, hrv, steps, timestamp = row
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    stale = datetime.now(timezone.utc) - dt > timedelta(minutes=10)

    return {
        "bpm": bpm,
        "hrv": hrv,
        "steps": steps,
        "timestamp": timestamp,
        "stale": stale,
    }


sse_clients = []
sse_lock = threading.Lock()


def _store_and_broadcast(bpm: int, hrv: float | None, steps: int | None, timestamp: str):
    conn.execute(
        "INSERT INTO hrm_data (bpm, hrv, steps, timestamp) VALUES (?, ?, ?, ?)",
        (bpm, hrv, steps, timestamp)
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

    return response


@app.post("/")
async def ingest(data: HrmInput, authorization: str = Header(None)):
    timestamp = data.timestamp or datetime.now(timezone.utc).isoformat()
    _store_and_broadcast(data.bpm, data.hrv, data.steps, timestamp)
    return {"ok": True}


@app.post("/log.php")
async def ingest_android(data: AndroidHrmInput, x_api_key: str = Header(None)):
    hrv = compute_rmssd(data.rr_intervals) if data.rr_intervals else None
    if data.recorded_at:
        timestamp = datetime.fromtimestamp(data.recorded_at / 1000, tz=timezone.utc).isoformat()
    else:
        timestamp = datetime.now(timezone.utc).isoformat()
    _store_and_broadcast(data.heart_rate, hrv, None, timestamp)
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
        "SELECT bpm, hrv, steps, timestamp FROM hrm_data WHERE timestamp >= ? ORDER BY id",
        (start.isoformat(),)
    )
    rows = cur.fetchall()
    return [
        {"bpm": r[0], "hrv": r[1], "steps": r[2], "timestamp": r[3]}
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

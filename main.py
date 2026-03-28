import os
import sqlite3
import json
import math
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

app = FastAPI()

DB_PATH = os.environ.get("DB_PATH", "hr.db")
HR_API_TOKEN = os.environ.get("HR_API_TOKEN", "change-me")
HRV_WINDOW = int(os.environ.get("HRV_WINDOW", "20"))

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("""
    CREATE TABLE IF NOT EXISTS hrm_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bpm INTEGER,
        hrv REAL,
        timestamp TEXT NOT NULL
    )
""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON hrm_data(timestamp)")

# Migrate old schemas if needed
try:
    conn.execute("ALTER TABLE hrm_data RENAME COLUMN hrv_rmssd TO hrv")
except Exception:
    pass

conn.commit()


class HrmInput(BaseModel):
    bpm: int
    hrv: float | None = None
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


rr_buffer: deque[list[int]] = deque(maxlen=HRV_WINDOW)


def rolling_hrv(rr_intervals: list[int] | None) -> float | None:
    if rr_intervals:
        rr_buffer.append(rr_intervals)
    all_rr = [val for sample in rr_buffer for val in sample]
    return compute_rmssd(all_rr)


def require_auth(authorization: str | None = None, x_api_key: str | None = None):
    if x_api_key and x_api_key == HR_API_TOKEN:
        return
    if authorization and authorization == f"Bearer {HR_API_TOKEN}":
        return
    raise HTTPException(status_code=401, detail="Invalid or missing token")


def get_response_data() -> dict:
    cur = conn.execute(
        "SELECT bpm, hrv, timestamp FROM hrm_data ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()
    count = conn.execute("SELECT COUNT(*) FROM hrm_data").fetchone()[0]

    if not row:
        return {"bpm": None, "hrv": None, "timestamp": None, "count": count, "stale": True}

    bpm, hrv, timestamp = row
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    stale = datetime.now(timezone.utc) - dt > timedelta(seconds=10)

    return {
        "bpm": bpm,
        "hrv": hrv,
        "timestamp": timestamp,
        "count": count,
        "stale": stale,
    }


sse_clients = []
sse_lock = threading.Lock()


def _store_and_broadcast(bpm: int, hrv: float | None, timestamp: str):
    conn.execute(
        "INSERT INTO hrm_data (bpm, hrv, timestamp) VALUES (?, ?, ?)",
        (bpm, hrv, timestamp)
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
async def ingest(data: HrmInput, authorization: str = Header(None), x_api_key: str = Header(None)):
    require_auth(authorization, x_api_key)
    timestamp = data.timestamp or datetime.now(timezone.utc).isoformat()
    _store_and_broadcast(data.bpm, data.hrv, timestamp)
    return {"ok": True}


@app.post("/log.php")
async def ingest_android(data: AndroidHrmInput, authorization: str = Header(None), x_api_key: str = Header(None)):
    require_auth(authorization, x_api_key)
    hrv = rolling_hrv(data.rr_intervals)
    if data.recorded_at:
        timestamp = datetime.fromtimestamp(data.recorded_at / 1000, tz=timezone.utc).isoformat()
    else:
        timestamp = datetime.now(timezone.utc).isoformat()
    _store_and_broadcast(data.heart_rate, hrv, timestamp)
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
        "SELECT bpm, hrv, timestamp FROM hrm_data WHERE timestamp >= ? ORDER BY id",
        (start.isoformat(),)
    )
    rows = cur.fetchall()
    return [
        {"bpm": r[0], "hrv": r[1], "timestamp": r[2]}
        for r in rows
    ]


@app.get("/test")
async def test(authorization: str = Header(None), x_api_key: str = Header(None)):
    valid = (x_api_key and x_api_key == HR_API_TOKEN) or (authorization and authorization == f"Bearer {HR_API_TOKEN}")
    return {
        "status": "ok",
        "token_valid": valid,
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

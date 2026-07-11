"""Serving API (FastAPI) -- kontrak C3 (docs/design/CONTRACTS.md).

REST /api/history/* baca Parquet curated (via WebHDFS, lihat hdfs_read.py) dan
MongoDB; WebSocket /ws/live forward MongoDB change stream (live_states,
zone_stats, alerts) ke semua klien yang terhubung, plus snapshot awal saat
connect.
"""
import asyncio
import re
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from common import load_config  # noqa: E402

from api.hdfs_read import ParquetPartitionNotFound, read_partition  # noqa: E402

cfg = load_config()
mongo_client = MongoClient(cfg["mongo"]["uri"])
db = mongo_client[cfg["mongo"]["db"]]

app = FastAPI(title="OpenSky Big Data Pipeline API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_WATCHED_COLLECTIONS = {"live_states", "zone_stats", "alerts"}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_refresh_lock = asyncio.Lock()


def _serialize_doc(doc):
    out = {}
    for k, v in doc.items():
        if k == "_id":
            out["_id"] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _serialize_live_state(doc):
    out = _serialize_doc(doc)
    out["icao24"] = out.pop("_id")
    return out


# ---------------------------------------------------------------------------
# REST -- kontrak C3
# ---------------------------------------------------------------------------


@app.get("/api/history/summary")
def history_summary(date: str):
    doc = db.daily_snapshot.find_one({"_id": date})
    if doc is None:
        raise HTTPException(status_code=404, detail=f"daily_snapshot belum ada untuk tanggal {date}")
    return _serialize_doc(doc)


@app.get("/api/history/hourly")
def history_hourly(date: str):
    try:
        table = read_partition(cfg["hdfs"]["base"], "states_clean", date)
    except ParquetPartitionNotFound:
        raise HTTPException(status_code=404, detail=f"Parquet states_clean belum ada untuk tanggal {date}")

    ts_list = table.column("ts").to_pylist()
    vel_list = table.column("velocity_ms").to_pylist()

    counts = [0] * 24
    vel_sums = [0.0] * 24
    vel_counts = [0] * 24
    for ts, vel in zip(ts_list, vel_list):
        h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
        counts[h] += 1
        if vel is not None:
            vel_sums[h] += vel
            vel_counts[h] += 1

    hours = [
        {
            "hour": h,
            "aircraft_count": counts[h],
            "avg_velocity_ms": round(vel_sums[h] / vel_counts[h], 1) if vel_counts[h] else None,
        }
        for h in range(24)
    ]
    return {"date": date, "hours": hours}


@app.get("/api/history/density")
def history_density(date: str):
    try:
        table = read_partition(cfg["hdfs"]["base"], "density_grid_hourly", date)
    except ParquetPartitionNotFound:
        raise HTTPException(status_code=404, detail=f"Parquet density_grid_hourly belum ada untuk tanggal {date}")

    zones = table.column("grid_cell").to_pylist()
    counts = table.column("aircraft_count").to_pylist()

    totals = {}
    for zone, count in zip(zones, counts):
        totals[zone] = totals.get(zone, 0) + count

    cells = []
    for zone, count in totals.items():
        lat_str, lon_str = zone.split("_")
        cells.append({"zone": zone, "lat": int(lat_str), "lon": int(lon_str), "count": count})
    return {"date": date, "cells": cells}


@app.post("/api/history/refresh")
async def history_refresh(date: str):
    if not _DATE_RE.match(date):
        raise HTTPException(status_code=400, detail="date harus format YYYY-MM-DD")

    if _refresh_lock.locked():
        raise HTTPException(status_code=409, detail="Refresh lain sedang berjalan, coba lagi sebentar")

    async with _refresh_lock:
        repo_root = Path(__file__).resolve().parent.parent
        try:
            # "spark-submit" alone won't launch here: it's a .cmd script, and
            # asyncio.create_subprocess_exec calls CreateProcess directly
            # (no shell), which doesn't do PATHEXT resolution the way an
            # interactive PowerShell/cmd session does.
            proc = await asyncio.create_subprocess_exec(
                "spark-submit.cmd",
                "src/batch_job.py",
                "--date",
                date,
                cwd=str(repo_root),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
        except (OSError, NotImplementedError) as exc:
            # NotImplementedError muncul kalau proses ini kebetulan jalan di
            # bawah asyncio.SelectorEventLoop (mis. uvicorn --reload di
            # Windows memaksa loop worker-nya jadi Selector, bukan Proactor)
            # -- Selector loop tidak bisa spawn subprocess sama sekali di
            # Windows. Jangan jalankan uvicorn dengan --reload di sini.
            raise HTTPException(status_code=500, detail=f"Gagal menjalankan spark-submit: {exc}")

    if proc.returncode == 2:
        # Exit code 2 = sentinel deterministik dari batch_job.py: raw_dir HDFS
        # untuk tanggal ini memang tidak ada (bukan kegagalan job). Jangan
        # coba tebak dari isi stderr -- pesan gagal Spark generik (mis. race
        # shutdown-hook deleteRecursively) berubah-ubah antar run dan tidak
        # bisa dipakai sebagai sinyal yang bisa diandalkan.
        raise HTTPException(
            status_code=404,
            detail=f"Belum ada data mentah (raw) untuk tanggal {date} -- ingest.py tidak berjalan pada tanggal itu",
        )
    if proc.returncode != 0:
        tail = stderr.decode(errors="replace")[-2000:]
        raise HTTPException(status_code=500, detail=f"batch_job.py gagal (exit {proc.returncode}): {tail}")

    return {"date": date, "status": "ok"}


@app.get("/api/live/states")
def live_states():
    docs = db.live_states.find({})
    return {"states": [_serialize_live_state(d) for d in docs]}


# ---------------------------------------------------------------------------
# WebSocket -- forward MongoDB change stream
# ---------------------------------------------------------------------------


class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active:
            self.active.remove(websocket)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()
_change_queue: asyncio.Queue = asyncio.Queue()
_main_loop: asyncio.AbstractEventLoop | None = None


def _watch_changes():
    """Jalan di thread terpisah (pymongo change stream sinkron). Simpan resume
    token supaya kalau stream putus (mis. error jaringan sesaat), reconnect
    lanjut dari event terakhir -- bukan dari awal lagi."""
    resume_token = None
    while True:
        try:
            kwargs = {"full_document": "updateLookup"}
            if resume_token is not None:
                kwargs["resume_after"] = resume_token
            with db.watch(**kwargs) as stream:
                for change in stream:
                    resume_token = change["_id"]
                    coll = change["ns"]["coll"]
                    if coll not in _WATCHED_COLLECTIONS:
                        continue
                    doc = change.get("fullDocument")
                    if doc is None:
                        continue
                    serialize = _serialize_live_state if coll == "live_states" else _serialize_doc
                    message = {"channel": coll, "data": serialize(doc)}
                    if _main_loop is not None:
                        _main_loop.call_soon_threadsafe(_change_queue.put_nowait, message)
        except Exception as exc:
            print(f"[ws] change stream error, retry in 3s: {exc}")
            time.sleep(3)


async def _broadcast_consumer():
    while True:
        message = await _change_queue.get()
        await manager.broadcast(message)


@app.on_event("startup")
async def on_startup():
    global _main_loop
    _main_loop = asyncio.get_event_loop()
    threading.Thread(target=_watch_changes, daemon=True).start()
    asyncio.create_task(_broadcast_consumer())


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        docs = db.live_states.find({})
        snapshot = {"channel": "snapshot", "data": {"states": [_serialize_live_state(d) for d in docs]}}
        await websocket.send_json(snapshot)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host=cfg["api"]["host"], port=cfg["api"]["port"], reload=True)

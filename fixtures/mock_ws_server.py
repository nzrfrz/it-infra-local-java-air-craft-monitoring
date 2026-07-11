"""WebSocket server mini yang mem-broadcast pesan format kontrak C3 tiap 2 detik,
dengan posisi pesawat bergeser sedikit tiap pesan — dipakai Track D untuk develop
frontend sebelum api/main.py jadi.

Usage: python fixtures/mock_ws_server.py
Connect: ws://localhost:8000/ws/live
"""
import asyncio
import json
import random
import time

import websockets

ZONES = ["-6_106", "-7_110", "-7_112"]
AIRLINES = ["GIA", "LNI", "BTK", "SJY"]
AIRCRAFT_COUNT = 15

state = {}
for i in range(AIRCRAFT_COUNT):
    icao24 = format(0x8A0000 + i, "x")
    lat_floor, lon_floor = (int(x) for x in ZONES[i % len(ZONES)].split("_"))
    state[icao24] = {
        "_id": icao24,
        "icao24": icao24,
        "callsign": f"{AIRLINES[i % len(AIRLINES)]}{100 + i}",
        "origin_country": "Indonesia",
        "lat": lat_floor - random.uniform(0, 0.9),
        "lon": lon_floor + random.uniform(0, 0.9),
        "baro_altitude_m": round(random.uniform(1000, 11000), 1),
        "velocity_ms": round(random.uniform(120, 250), 1),
        "true_track": round(random.uniform(0, 359), 1),
        "on_ground": False,
        "squawk": "1200",
    }

CLIENTS = set()


def step(doc):
    doc["lat"] += 0.005 * (1 if 90 < doc["true_track"] < 270 else -1)
    doc["lon"] += 0.005 * (1 if doc["true_track"] < 180 else -1)
    doc["ts"] = int(time.time())
    return doc


async def broadcaster():
    while True:
        await asyncio.sleep(2)
        if not CLIENTS:
            continue
        icao24 = random.choice(list(state.keys()))
        doc = step(state[icao24])
        msg = json.dumps({"channel": "live_states", "data": doc})
        await asyncio.gather(*(c.send(msg) for c in CLIENTS), return_exceptions=True)

        if random.random() < 0.05:
            alert = {
                "type": "emergency_squawk",
                "icao24": icao24,
                "zone": None,
                "ts": int(time.time()),
                "severity": "critical",
                "details": "squawk 7700 (simulasi)",
            }
            alert_msg = json.dumps({"channel": "alerts", "data": alert})
            await asyncio.gather(*(c.send(alert_msg) for c in CLIENTS), return_exceptions=True)


async def handler(websocket):
    CLIENTS.add(websocket)
    try:
        snapshot = json.dumps({"channel": "snapshot", "data": {"states": list(state.values())}})
        await websocket.send(snapshot)
        async for _ in websocket:
            pass
    finally:
        CLIENTS.discard(websocket)


async def main():
    async with websockets.serve(handler, "localhost", 8000):
        print("mock_ws_server jalan di ws://localhost:8000/ws/live (path diabaikan, semua konek diterima)")
        await broadcaster()


if __name__ == "__main__":
    asyncio.run(main())

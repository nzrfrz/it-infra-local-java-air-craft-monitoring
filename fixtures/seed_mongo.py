"""Isi 4 koleksi MongoDB (kontrak C2) dengan data dummy untuk dev Track C & D
tanpa perlu menunggu Track A/B selesai.

Jalankan berulang untuk membuat live_states "bergerak" (posisi bergeser tiap run).

Usage: python fixtures/seed_mongo.py
"""
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pymongo import MongoClient, ReplaceOne

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "config.yaml"

ZONES = ["-6_106", "-7_110", "-7_112"]
AIRLINES = ["GIA", "LNI", "BTK", "SJY"]
AIRCRAFT_COUNT = 20


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def seed_live_states(db, now_ts):
    ops = []
    for i in range(AIRCRAFT_COUNT):
        icao24 = format(0x8A0000 + i, "x")
        zone = ZONES[i % len(ZONES)]
        lat_floor, lon_floor = (int(x) for x in zone.split("_"))
        doc = {
            "_id": icao24,
            "callsign": f"{AIRLINES[i % len(AIRLINES)]}{100 + i}",
            "origin_country": "Indonesia",
            "lat": lat_floor - random.uniform(0, 0.9),
            "lon": lon_floor + random.uniform(0, 0.9),
            "baro_altitude_m": round(random.uniform(1000, 11000), 1),
            "velocity_ms": round(random.uniform(120, 250), 1),
            "true_track": round(random.uniform(0, 359), 1),
            "on_ground": False,
            "squawk": "1200",
            "ts": now_ts,
            "updated_at": datetime.now(timezone.utc),
        }
        ops.append(ReplaceOne({"_id": icao24}, doc, upsert=True))
    db.live_states.bulk_write(ops)


def seed_zone_stats(db, now_ts):
    window_start = now_ts - (now_ts % 120)
    ops = []
    for zone in ZONES:
        doc = {
            "_id": f"{zone}_{window_start}",
            "zone": zone,
            "window_start": window_start,
            "window_end": window_start + 120,
            "aircraft_count": random.randint(3, 15),
            "avg_velocity_ms": round(random.uniform(150, 220), 1),
            "updated_at": datetime.now(timezone.utc),
        }
        ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
    db.zone_stats.bulk_write(ops)


def seed_alerts(db, now_ts):
    alerts = [
        {
            "type": "emergency_squawk",
            "icao24": format(0x8A0000 + 2, "x"),
            "zone": None,
            "ts": now_ts,
            "severity": "critical",
            "details": "squawk 7700",
            "created_at": datetime.now(timezone.utc),
        },
        {
            "type": "density_spike",
            "icao24": None,
            "zone": ZONES[0],
            "ts": now_ts,
            "severity": "warning",
            "details": "kepadatan zona > baseline",
            "created_at": datetime.now(timezone.utc),
        },
    ]
    db.alerts.insert_many(alerts)


def seed_daily_snapshot(db, today):
    doc = {
        "_id": today,
        "total_records": 184201,
        "unique_aircraft": AIRCRAFT_COUNT,
        "busiest_hour": 13,
        "top_airlines": [{"prefix": a, "count": random.randint(100, 1200)} for a in AIRLINES],
        "generated_at": datetime.now(timezone.utc),
    }
    db.daily_snapshot.replace_one({"_id": today}, doc, upsert=True)


def main():
    cfg = load_config()
    client = MongoClient(cfg["mongo"]["uri"])
    db = client[cfg["mongo"]["db"]]

    now_ts = int(time.time())
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    seed_live_states(db, now_ts)
    seed_zone_stats(db, now_ts)
    seed_alerts(db, now_ts)
    seed_daily_snapshot(db, today)

    print(f"Seeded {AIRCRAFT_COUNT} live_states, {len(ZONES)} zone_stats, 2 alerts, 1 daily_snapshot ({today})")


if __name__ == "__main__":
    main()

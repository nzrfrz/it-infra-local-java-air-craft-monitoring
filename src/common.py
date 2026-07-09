"""Fungsi bersama dipakai oleh ingest.py dan batch_job.py — dijaga tetap DRY
sesuai kontrak C1 (docs/design/CONTRACTS.md)."""
import math
from pathlib import Path

import yaml

# Index kolom array `states` pada respons OpenSky /api/states/all
_IDX_ICAO24 = 0
_IDX_CALLSIGN = 1
_IDX_ORIGIN_COUNTRY = 2
_IDX_TIME_POSITION = 3
_IDX_LON = 5
_IDX_LAT = 6
_IDX_BARO_ALTITUDE = 7
_IDX_ON_GROUND = 8
_IDX_VELOCITY = 9
_IDX_TRUE_TRACK = 10
_IDX_VERTICAL_RATE = 11
_IDX_SQUAWK = 14


def load_config(path=None):
    if path is None:
        path = Path(__file__).resolve().parent.parent / "config" / "config.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def zone_for(lat, lon):
    """Grid 1x1 derajat, format '{floor(lat)}_{floor(lon)}' (mis. '-7_110')."""
    return f"{math.floor(lat)}_{math.floor(lon)}"


def flatten(raw, tier, fetched_at):
    """Ubah respons mentah OpenSky (dict dengan key 'states') menjadi list dict
    sesuai kontrak C1. Baris tanpa lat/lon/ts dibuang."""
    states = (raw or {}).get("states") or []
    out = []
    for s in states:
        icao24 = s[_IDX_ICAO24]
        ts = s[_IDX_TIME_POSITION]
        lat = s[_IDX_LAT]
        lon = s[_IDX_LON]
        if icao24 is None or ts is None or lat is None or lon is None:
            continue
        callsign = s[_IDX_CALLSIGN]
        out.append(
            {
                "icao24": icao24,
                "callsign": callsign.strip() if callsign else None,
                "origin_country": s[_IDX_ORIGIN_COUNTRY],
                "ts": int(ts),
                "lat": lat,
                "lon": lon,
                "baro_altitude_m": s[_IDX_BARO_ALTITUDE],
                "velocity_ms": s[_IDX_VELOCITY],
                "true_track": s[_IDX_TRUE_TRACK],
                "vertical_rate": s[_IDX_VERTICAL_RATE],
                "on_ground": bool(s[_IDX_ON_GROUND]) if s[_IDX_ON_GROUND] is not None else None,
                "squawk": s[_IDX_SQUAWK],
                "tier": tier,
                "fetched_at": fetched_at,
            }
        )
    return out

"""Fungsi bersama dipakai oleh ingest.py, streaming_job.py dan batch_job.py —
dijaga tetap DRY sesuai kontrak C1 (docs/design/CONTRACTS.md)."""
import math
from pathlib import Path

import yaml
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

# Skema baris NDJSON/DataFrame sesuai kontrak C1 (dipakai streaming_job & batch_job)
STATES_SCHEMA = StructType(
    [
        StructField("icao24", StringType(), False),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("ts", LongType(), False),
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False),
        StructField("baro_altitude_m", DoubleType(), True),
        StructField("velocity_ms", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("squawk", StringType(), True),
        StructField("tier", StringType(), False),
        StructField("fetched_at", LongType(), True),
    ]
)

# Bandara utama & sekitarnya (lat, lon) untuk agregasi airport_hourly
AIRPORTS = {
    "CGK": (-6.1256, 106.6559),
    "HLP": (-6.2665, 106.8909),
    "SUB": (-7.3798, 112.7869),
    "JOG": (-7.7881, 110.4317),
    "BDO": (-6.9006, 107.5762),
}
AIRPORT_RADIUS_DEG = 0.5

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


def _as_float(v):
    return float(v) if v is not None else None


def flatten(raw, tier, fetched_at):
    """Ubah respons mentah OpenSky (dict dengan key 'states') menjadi list dict
    sesuai kontrak C1. Baris tanpa lat/lon/ts dibuang. Field numerik dipaksa
    float agar cocok dengan DoubleType saat dipakai bangun Spark DataFrame
    (createDataFrame dgn skema eksplisit tidak auto-cast int->double)."""
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
                "lat": _as_float(lat),
                "lon": _as_float(lon),
                "baro_altitude_m": _as_float(s[_IDX_BARO_ALTITUDE]),
                "velocity_ms": _as_float(s[_IDX_VELOCITY]),
                "true_track": _as_float(s[_IDX_TRUE_TRACK]),
                "vertical_rate": _as_float(s[_IDX_VERTICAL_RATE]),
                "on_ground": bool(s[_IDX_ON_GROUND]) if s[_IDX_ON_GROUND] is not None else None,
                "squawk": s[_IDX_SQUAWK],
                "tier": tier,
                "fetched_at": fetched_at,
            }
        )
    return out

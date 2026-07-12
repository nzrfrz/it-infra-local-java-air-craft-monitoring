"""Fungsi bersama dipakai oleh ingest.py, streaming_job.py dan batch_job.py —
dijaga tetap DRY sesuai kontrak C1 (docs/design/CONTRACTS.md)."""
import math
from pathlib import Path

import yaml
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

# Skema baris NDJSON/DataFrame sesuai kontrak C1 (dipakai streaming_job & batch_job).
# Ini "kontrak data" antar komponen: siapa pun yang menulis/membaca file
# landing_stream/ atau membangun DataFrame Spark WAJIB memakai skema yang
# sama persis ini, supaya streaming & batch job selalu kompatibel.
STATES_SCHEMA = StructType(
    [
        StructField("icao24", StringType(), False),       # ID unik transponder pesawat (kunci utama)
        StructField("callsign", StringType(), True),       # nomor penerbangan, mis. "GIA123 " (bisa null)
        StructField("origin_country", StringType(), True), # negara registrasi pesawat
        StructField("ts", LongType(), False),               # epoch detik saat posisi ini tercatat
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False),
        StructField("baro_altitude_m", DoubleType(), True),
        StructField("velocity_ms", DoubleType(), True),
        StructField("true_track", DoubleType(), True),     # heading/arah hadap pesawat (derajat)
        StructField("vertical_rate", DoubleType(), True),  # laju naik/turun (m/s), proxy fase terbang
        StructField("on_ground", BooleanType(), True),
        StructField("squawk", StringType(), True),         # kode transponder; 7500/7600/7700 = darurat
        StructField("tier", StringType(), False),           # dari tier mana data ini diambil ("java"/"national")
        StructField("fetched_at", LongType(), True),         # epoch detik saat ingest.py melakukan request
    ]
)

# Bandara utama & sekitarnya (lat, lon) untuk agregasi airport_hourly di batch_job.py.
AIRPORTS = {
    "CGK": (-6.1256, 106.6559),  # Soekarno-Hatta, Jakarta
    "HLP": (-6.2665, 106.8909),  # Halim Perdanakusuma, Jakarta
    "SUB": (-7.3798, 112.7869),  # Juanda, Surabaya
    "JOG": (-7.7881, 110.4317),  # Adisutjipto/YIA area, Yogyakarta
    "BDO": (-6.9006, 107.5762),  # Husein Sastranegara, Bandung
}
AIRPORT_RADIUS_DEG = 0.5  # radius "dekat bandara" dalam derajat lat/lon (~55 km)

# Index kolom array `states` pada respons OpenSky /api/states/all -- API
# mengembalikan tiap pesawat sebagai ARRAY posisi tetap (bukan objek dengan
# nama field), jadi kita harus tahu persis index kolom mana berisi apa.
# (Beberapa index dari spesifikasi OpenSky sengaja tidak dipakai di sini,
# mis. index 4 "last_contact" dan index 12-13 "geo_altitude"/"spi".)
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
    """Baca config/config.yaml (bounding box tier, kredensial OpenSky, path
    HDFS/Mongo). Kalau path tidak diberikan, cari otomatis relatif terhadap
    lokasi file ini (supaya konsisten dipanggil dari direktori mana pun)."""
    if path is None:
        path = Path(__file__).resolve().parent.parent / "config" / "config.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def zone_for(lat, lon):
    """Grid 1x1 derajat, format '{floor(lat)}_{floor(lon)}' (mis. '-7_110').
    Ini cara sederhana membagi ruang udara jadi sel-sel untuk agregasi
    kepadatan per zona (dipakai streaming_job.py untuk zone_stats)."""
    return f"{math.floor(lat)}_{math.floor(lon)}"


def _as_float(v):
    """None tetap None (bukan error/0) supaya nilai yang memang tidak
    tersedia dari OpenSky (mis. pesawat di darat tanpa data kecepatan)
    tidak salah diartikan sebagai 0."""
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
        # Baris tanpa 4 kolom wajib ini tidak berguna untuk analisis apa pun
        # (tidak tahu pesawat mana, di mana, kapan) -- dibuang di sini,
        # sebelum masuk ke pipeline manapun, bukan ditangani belakangan.
        if icao24 is None or ts is None or lat is None or lon is None:
            continue
        callsign = s[_IDX_CALLSIGN]
        out.append(
            {
                "icao24": icao24,
                "callsign": callsign.strip() if callsign else None,  # OpenSky selalu pad callsign dgn spasi
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

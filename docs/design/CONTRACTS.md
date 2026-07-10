# Kontrak Data — Pipeline OpenSky

> Kontrak ini adalah "perjanjian" antar komponen pipeline. **Tidak boleh diubah sepihak** — perubahan field/skema harus disinkronkan di semua konsumen (ingest, batch, streaming, API, frontend) sekaligus.

## Batasan Global

- Semua path & kredensial dibaca dari `config/config.yaml` — tidak ada hardcode.
- Format file landing: **NDJSON** (1 baris = 1 pesawat).
- Polling: Jawa 60 dtk, nasional 15 mnt (kuota ≈3.264/4.000 kredit/hari — jangan dipersempit).
- Zona = grid 1°×1°, string `"{floor(lat)}_{floor(lon)}"` (contoh `-7_110`).
- Timestamp semua epoch detik (int), UTC.

## C1 — Format file landing (NDJSON)

Nama file: `states_{tier}_{YYYYmmddTHHMMSS}.json`, 1 baris JSON per pesawat:

```json
{"icao24":"8a06f1","callsign":"GIA123","origin_country":"Indonesia","ts":1751970000,"lat":-6.12,"lon":106.65,"baro_altitude_m":3500.0,"velocity_ms":180.5,"true_track":270.1,"vertical_rate":-2.5,"on_ground":false,"squawk":"3421","tier":"java","fetched_at":1751970005}
```

Field boleh `null` kecuali `icao24`, `ts`, `lat`, `lon`, `tier`. Raw HDFS menyimpan respons API asli (belum flatten); flatten dilakukan `common.flatten()` untuk landing.

## C2 — Skema koleksi MongoDB

```js
// live_states  (_id = icao24, TTL index on updated_at: 3600s)
{ _id:"8a06f1", callsign:"GIA123", origin_country:"Indonesia", lat:-6.12, lon:106.65,
  baro_altitude_m:3500.0, velocity_ms:180.5, true_track:270.1, on_ground:false,
  squawk:"3421", ts:1751970000, updated_at:ISODate() }

// zone_stats  (_id = "{zone}_{window_start}", TTL 24h)
{ _id:"-7_110_1751970000", zone:"-7_110", window_start:1751970000, window_end:1751970120,
  aircraft_count:12, avg_velocity_ms:175.3, updated_at:ISODate() }

// alerts
{ type:"emergency_squawk"|"density_spike", icao24:"8a06f1"|null, zone:"-7_110"|null,
  ts:1751970000, severity:"critical"|"warning", details:"squawk 7700", created_at:ISODate() }

// daily_snapshot (_id = "YYYY-MM-DD")
{ _id:"2026-07-08", total_records:184201, unique_aircraft:412, busiest_hour:13,
  top_airlines:[{prefix:"GIA",count:1204},{prefix:"LNI",count:998}], generated_at:ISODate() }
```

## C3 — Kontrak API (FastAPI)

```
GET /api/history/summary?date=YYYY-MM-DD   → dokumen daily_snapshot (404 jika belum ada)
GET /api/history/hourly?date=YYYY-MM-DD    → {date, hours:[{hour:0-23, aircraft_count, avg_velocity_ms}]}
GET /api/history/density?date=YYYY-MM-DD   → {date, cells:[{zone:"-7_110", lat:-7, lon:110, count}]}
POST /api/history/refresh?date=YYYY-MM-DD  → jalankan batch_job.py utk tanggal itu (blocking, ~30s), balas {date, status:"ok"} (400 date invalid, 409 refresh lain masih jalan, 500 batch gagal)
GET /api/live/states                       → {states:[<dokumen live_states>]}  (snapshot awal utk frontend)
WS  /ws/live                               → pesan JSON: {"channel":"live_states"|"zone_stats"|"alerts"|"snapshot", "data":{<dokumen>}}
```

## C4 — Skema Parquet curated

Sesuai §4 dokumen desain arsitektur. Kolom `grid_cell` memakai aturan zona di Batasan Global.

| Dataset | Partisi | Isi |
|---|---|---|
| `states_clean` | `dt` | 1 baris = 1 pesawat @ 1 waktu, kolom sesuai C1 + `grid_cell` |
| `density_grid_hourly` | `dt` | zone × hour × count |
| `airport_hourly` | `dt` | count dalam radius 0.5° dari CGK/HLP/SUB/JOG/BDO per jam |
| `daily_summary` | `dt` | total, unique aircraft, busiest hour, top-10 prefix callsign |

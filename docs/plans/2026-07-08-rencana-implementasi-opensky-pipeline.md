# Rencana Implementasi: Pipeline Big Data Lalu Lintas Udara (OpenSky)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pipeline end-to-end OpenSky → HDFS/MongoDB → Spark batch + Structured Streaming → FastAPI → dashboard React, siap didemokan & dipresentasikan.

**Arsitektur:** Pola Lambda sesuai [dokumen desain](../design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md). Ingestor menulis NDJSON ke HDFS (arsip) + folder landing (streaming). Spark batch menghasilkan Parquet curated; Structured Streaming menghasilkan agregat & alert ke MongoDB. FastAPI mem-forward change stream via WebSocket; React menampilkan peta live.

**Tech Stack:** Python 3.10+, PySpark, Hadoop/HDFS (terinstall), MongoDB ≥6 (replica set 1 node), FastAPI + uvicorn, React 18 + Vite + deck.gl + MapLibre.

## Batasan Global

- Semua path & kredensial dibaca dari `config/config.yaml` — tidak ada hardcode.
- Format file landing: **NDJSON** (1 baris = 1 pesawat), skema di Kontrak C1.
- Polling: Jawa 60 dtk, nasional 15 mnt (kuota 3.264/4.000 kredit — jangan dipersempit).
- Zona = grid 1°×1°, string `"{floor(lat)}_{floor(lon)}"` (contoh `-7_110`).
- Timestamp semua epoch detik (int), UTC.
- Commit kecil & sering; branch per track: `track-a-ingest`, `track-b-spark`, `track-c-api`, `track-d-web`.

---

## Peta Track & Dependensi

```
Track 0: Fondasi & Kontrak (dikerjakan BERSAMA dulu, ±1-2 jam)
   │
   ├──► Track A: Ingestion & Infra (Orang 1)      ─┐
   ├──► Track B: Spark Batch + Streaming (Orang 2) ─┼─► M1 ► M2 ► M3 ► M4 (integrasi & demo)
   ├──► Track C: Serving API FastAPI (Orang 3)     ─┤
   └──► Track D: Frontend React (Orang 4/3)        ─┘
```

Track A–D **independen penuh** setelah Track 0 selesai, karena masing-masing develop melawan **fixtures** (data contoh), bukan melawan output track lain. Integrasi terjadi di milestone M1–M4.

**Kalau hanya 2 orang:** Orang 1 = Track A + B (jalur data), Orang 2 = Track C + D (jalur serving). Kalau 3 orang: A+B, C, D.

---

## Track 0: Fondasi & Kontrak (bersama, blocking)

### Task 0.1 — Scaffold repo & git

- [ ] `git init`, buat `.gitignore` (isi: `venv/`, `node_modules/`, `config/config.yaml`, `landing_stream/`, `recordings/`, `__pycache__/`, `dist/`, `*.log`)
- [ ] Buat struktur folder: `src/ api/ web/ config/ scripts/ fixtures/ docs/`
- [ ] Buat `config/config.example.yaml` (di-commit; `config.yaml` asli tidak):

```yaml
opensky:
  client_id: "ISI_CLIENT_ID"
  client_secret: "ISI_CLIENT_SECRET"
  token_url: "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
  api_url: "https://opensky-network.org/api/states/all"
  tiers:
    java:     { lamin: -9.0,  lamax: -5.0, lomin: 105.0, lomax: 115.0, interval_s: 60 }
    national: { lamin: -11.0, lamax: 6.0,  lomin: 95.0,  lomax: 141.0, interval_s: 900 }
hdfs:
  base: "hdfs://localhost:9000/bigdata/opensky"
paths:
  landing_stream: "D:/bigdata/landing_stream"
  recordings: "D:/bigdata/recordings"
  checkpoint: "hdfs://localhost:9000/bigdata/opensky/checkpoints/streaming_opensky"
mongo:
  uri: "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
  db: "opensky"
api:
  host: "0.0.0.0"
  port: 8000
```

- [ ] Buat `requirements.txt`: `pyspark, pymongo, requests, fastapi, uvicorn[standard], pyarrow, pyyaml, websockets`
- [ ] Push ke GitHub (repo private), invite semua anggota, buat 4 branch track
- [ ] Setiap anggota daftar akun **opensky-network.org** → buat API client → simpan client_id/secret masing-masing di `config.yaml` lokal

### Task 0.2 — Kontrak data (file `docs/design/CONTRACTS.md`, di-commit)

Salin definisi berikut apa adanya — ini "perjanjian" antar track, **tidak boleh diubah sepihak**:

**C1. Format file landing (NDJSON)** — nama file `states_{tier}_{YYYYmmddTHHMMSS}.json`, 1 baris per pesawat:

```json
{"icao24":"8a06f1","callsign":"GIA123","origin_country":"Indonesia","ts":1751970000,"lat":-6.12,"lon":106.65,"baro_altitude_m":3500.0,"velocity_ms":180.5,"true_track":270.1,"vertical_rate":-2.5,"on_ground":false,"squawk":"3421","tier":"java","fetched_at":1751970005}
```

Field boleh `null` kecuali `icao24`, `ts`, `lat`, `lon`, `tier`. Raw HDFS menyimpan respons API asli (belum flatten); flatten dilakukan ingestor untuk landing.

**C2. Skema koleksi MongoDB**

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

**C3. Kontrak API (FastAPI)**

```
GET /api/history/summary?date=YYYY-MM-DD   → dokumen daily_snapshot (404 jika belum ada)
GET /api/history/hourly?date=YYYY-MM-DD    → {date, hours:[{hour:0-23, aircraft_count, avg_velocity_ms}]}
GET /api/history/density?date=YYYY-MM-DD   → {date, cells:[{zone:"-7_110", lat:-7, lon:110, count}]}
GET /api/live/states                       → {states:[<dokumen live_states>]}  (snapshot awal utk frontend)
WS  /ws/live                               → pesan JSON: {"channel":"live_states"|"zone_stats"|"alerts", "data":{<dokumen>}}
```

**C4. Skema Parquet curated** — sesuai §4 dokumen desain (`states_clean`, `density_grid_hourly`, `airport_hourly`, `daily_summary`). Kolom `grid_cell` memakai aturan zona Batasan Global.

### Task 0.3 — Fixtures (supaya track bisa paralel)

- [ ] `fixtures/sample_landing.json` — NDJSON ±30 baris sesuai C1, variasi 3 zona, sertakan 1 baris `squawk:"7700"` dan 2 baris duplikat `(icao24, ts)` (untuk uji dedup)
- [ ] `fixtures/seed_mongo.py` — skrip yang mengisi 4 koleksi C2 dengan data dummy (±20 live_states yang bergerak jika dijalankan berulang, 5 zone_stats, 2 alerts, 1 daily_snapshot) — dipakai Track C & D sebelum Spark jadi
- [ ] `fixtures/mock_ws_server.py` — WebSocket server mini (library `websockets`) yang mem-broadcast pesan format C3 tiap 2 detik dengan posisi pesawat bergeser — dipakai Track D sebelum API jadi
- [ ] Commit semua ke `main` sebelum berpencar

---

## Track A: Ingestion & Infra (Orang 1)

### Task A.1 — Setup infrastruktur lokal

- [ ] Verifikasi HDFS jalan: `hdfs dfsadmin -report` (NameNode + DataNode up)
- [ ] Buat direktori HDFS: `hdfs dfs -mkdir -p /bigdata/opensky/raw /bigdata/opensky/curated /bigdata/opensky/checkpoints`
- [ ] Install MongoDB ≥6, jalankan sebagai replica set 1 node: tambah `replication: {replSetName: rs0}` di `mongod.cfg`, restart service, lalu `mongosh --eval "rs.initiate()"`
- [ ] Verifikasi change stream jalan: di `mongosh`, `db.test.watch()` tidak error
- [ ] Buat folder lokal `D:/bigdata/landing_stream` dan `D:/bigdata/recordings`
- [ ] Tulis `scripts/start_infra.ps1` (start HDFS + cek Mongo service + print status) dan commit

### Task A.2 — `src/ingest.py` (poller 2-tier)

- [ ] Fungsi `get_token(cfg)` — OAuth2 client-credentials ke OpenSky, cache token sampai expiry
- [ ] Fungsi `fetch_states(cfg, tier)` — GET `/states/all` dengan bbox tier; retry 3× exponential backoff (2/4/8 dtk); return respons mentah
- [ ] Fungsi `flatten(raw, tier)` — array `states` OpenSky → list dict sesuai **kontrak C1** (mapping index: 0=icao24, 1=callsign strip, 2=origin_country, 3=time_position→ts, 5=lon, 6=lat, 7=baro_altitude, 8=on_ground, 9=velocity, 10=true_track, 11=vertical_rate, 14=squawk); buang baris tanpa lat/lon/ts
- [ ] Fungsi `write_outputs(raw, flat, tier, cfg)` — (1) raw JSON → HDFS `raw/dt=YYYY-MM-DD/` via WebHDFS/`hdfs dfs -put`; (2) NDJSON → `landing_stream/` dengan **tulis ke file `.tmp` lalu rename** (wajib — agar Spark tidak membaca file setengah jadi); (3) salinan NDJSON → `recordings/`
- [ ] Loop utama: scheduler 2 tier sesuai interval config; log jumlah pesawat + kredit terpakai kumulatif per hari
- [ ] Uji unit `flatten()` dengan respons contoh yang disimpan sebagai `fixtures/sample_opensky_response.json`; uji manual: jalankan 3 menit, cek ≥3 file di landing + file muncul di `hdfs dfs -ls`
- [ ] Commit

### Task A.3 — `src/replay.py` (fallback demo offline)

- [ ] Baca semua file di `recordings/` terurut nama, salin ke `landing_stream/` sesuai selisih timestamp asli; flag `--speed 5` untuk dipercepat; pola tmp+rename sama dengan A.2
- [ ] Uji: rekam ≥10 menit data riil, jalankan replay `--speed 10`, pastikan file muncul berurutan
- [ ] Commit; merge `track-a-ingest` → `main` (PR, minta 1 review teman)

---

## Track B: Spark Batch + Streaming (Orang 2)

> Develop pakai `fixtures/sample_landing.json` — tidak perlu menunggu Track A. Jalankan Spark lokal: `spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 ...`

### Task B.1 — `src/streaming_job.py`

- [ ] Definisikan `StructType` schema eksplisit sesuai C1 (jangan inferSchema pada streaming)
- [ ] `readStream.format("json").schema(...).option("maxFilesPerTrigger",10).load(landing_stream)`
- [ ] Transformasi: tambah kolom `zone` (floor lat/lon), `event_time` dari `ts`
- [ ] **Query 1 — live_states:** `foreachBatch` → upsert ke Mongo `live_states` by `icao24` (pymongo `ReplaceOne(upsert=True)` per batch)
- [ ] **Query 2 — zone_stats:** `groupBy(window(event_time,"2 minutes"), zone)` + watermark 1 menit → count & avg velocity → upsert `_id="{zone}_{window_start}"`
- [ ] **Query 3 — alerts:** filter `squawk in (7500,7600,7700)` → insert `alerts` type `emergency_squawk`; deteksi `density_spike`: count zona window ini > 3× rata-rata 1 jam terakhir (baca baseline dari `zone_stats` di foreachBatch)
- [ ] Checkpoint per query ke subfolder `checkpoint/{q1,q2,q3}`
- [ ] Uji dengan fixture: salin `sample_landing.json` ke landing → cek 4 hal: dokumen live_states muncul, zone_stats terisi, alert squawk 7700 tercipta, duplikat tidak menggandakan count
- [ ] Uji recovery: kill proses, salin file baru, restart → tidak ada duplikat/error
- [ ] Commit

### Task B.2 — `src/batch_job.py`

- [ ] Baca `raw/dt=<tanggal>` (argumen `--date`, default kemarin) → flatten (pakai fungsi yang sama dengan ingest — pindahkan `flatten()` ke `src/common.py` agar DRY)
- [ ] Cleaning: dedup `(icao24, ts)`, filter koordinat dalam bbox nasional, altitude -100..15000 m
- [ ] Tulis `states_clean` Parquet partisi `dt`
- [ ] Agregasi → Parquet: `density_grid_hourly` (zone × hour × count), `airport_hourly` (count dalam radius 0.5° dari CGK/HLP/SUB/JOG/BDO per jam), `daily_summary` (total, unique aircraft, busiest hour, top-10 prefix callsign 3 huruf)
- [ ] Tulis `daily_snapshot` ke Mongo sesuai C2
- [ ] Uji dengan fixture sebagai raw input: baris duplikat hilang, jumlah agregat benar (hitung manual dari fixture), Parquet terbaca balik via `spark.read.parquet`
- [ ] Tulis `scripts/run_batch_daily.ps1` + daftarkan ke Task Scheduler (jam 01.00)
- [ ] Commit; merge `track-b-spark` → `main` (PR + review)

---

## Track C: Serving API (Orang 3)

> Develop pakai `fixtures/seed_mongo.py` — tidak perlu menunggu Track B. Butuh MongoDB jalan (koordinasi dgn A.1, atau install Mongo sendiri dulu).

### Task C.1 — `api/main.py` REST

- [ ] Endpoint sesuai **kontrak C3**: `/api/history/summary`, `/api/history/hourly`, `/api/history/density`, `/api/live/states`
- [ ] Sumber data: summary & live dari Mongo; hourly & density dari Parquet curated via pyarrow (`pyarrow.parquet.read_table` + filter) — jika Parquet belum ada, fallback 404 dengan pesan jelas
- [ ] CORS middleware allow `http://localhost:5173`
- [ ] Uji: seed Mongo → `curl` tiap endpoint, bandingkan respons dengan kontrak C3 field per field
- [ ] Commit

### Task C.2 — WebSocket change stream

- [ ] `/ws/live`: saat connect kirim `{"channel":"snapshot","data":{"states":[...]}}` (isi live_states saat ini), lalu task async `collection.watch()` pada 3 koleksi → forward tiap perubahan sebagai pesan C3
- [ ] Connection manager: multi-klien, cleanup saat disconnect, change stream resume token disimpan agar reconnect tidak kehilangan event
- [ ] Uji: jalankan `fixtures/seed_mongo.py` berulang di terminal lain → klien uji (`python -m websockets ws://localhost:8000/ws/live`) menerima push
- [ ] Commit; merge `track-c-api` → `main` (PR + review)

---

## Track D: Frontend React (Orang 4, atau Orang 3 setelah C selesai)

> Develop pakai `fixtures/mock_ws_server.py` + respons REST contoh — tidak perlu menunggu Track C.

### Task D.1 — Scaffold & peta live

- [ ] `npm create vite@latest web -- --template react-ts`; install `deck.gl @deck.gl/react maplibre-gl react-map-gl recharts`
- [ ] Komponen `LiveMap`: MapLibre basemap (style demo gratis `https://demotiles.maplibre.org/style.json` atau OSM raster) + deck.gl `IconLayer`/`ScatterplotLayer` posisi pesawat, rotasi ikon dari `true_track`, tooltip callsign/altitude/velocity
- [ ] Hook `useLiveSocket(url)`: connect WS, proses `snapshot` lalu pesan `live_states` inkremental → state `Map<icao24, state>`; auto-reconnect exponential backoff
- [ ] Uji melawan `mock_ws_server.py`: marker bergerak mulus tanpa refresh halaman
- [ ] Commit

### Task D.2 — Panel historis & alert

- [ ] Panel alert: list dari pesan channel `alerts` (badge merah utk `critical`), toast saat alert baru masuk
- [ ] Panel historis (route/tab kedua): fetch REST C3 → bar chart per jam (recharts), heatmap kepadatan zona (deck.gl `HeatmapLayer` dari `/api/history/density`), kartu ringkasan dari `/summary`
- [ ] Kartu status header: jumlah pesawat live, waktu update terakhir, indikator koneksi WS
- [ ] Uji semua panel dengan mock/seed data; commit; merge `track-d-web` → `main` (PR + review)

---

## Milestone Integrasi (bersama, berurutan)

### M1 — Jalur streaming end-to-end (A + B)

- [ ] Jalankan `start_infra.ps1` → `ingest.py` → `streaming_job.py` dengan **data riil** minimal 30 menit
- [ ] Verifikasi: file landing bertambah tiap 60 dtk; `live_states` terisi & ter-update; Spark UI tab Streaming sehat (batch duration < trigger interval); tidak ada kredit API terbuang (cek log)
- [ ] Biarkan ingest jalan terus (idealnya 1–3 hari) untuk menimbun data raw buat batch & demo

### M2 — Jalur serving end-to-end (C + D di atas M1)

- [ ] Start FastAPI + `npm run dev` → peta menampilkan **pesawat riil di atas Jawa bergerak tanpa refresh**
- [ ] Uji alert: jalankan `replay.py` atas rekaman yang di-edit menyisipkan squawk 7700 → toast muncul di dashboard

### M3 — Jalur batch (B di atas data M1)

- [ ] `batch_job.py --date <kemarin>` atas data riil → panel historis dashboard terisi (chart per jam, heatmap, kartu ringkasan)
- [ ] Cek angka masuk akal: bandingkan total record dengan hitungan `hdfs dfs -cat ... | wc -l` sampel

### M4 — Validasi resiliensi + persiapan presentasi

- [ ] Jalankan 3 eksperimen chaos (§6 desain): kill DataNode saat batch; inject latensi/error di ingest; kill executor saat streaming. Catat hasil (hipotesis terbukti?, MTTR) di `docs/design/hasil-eksperimen-resiliensi.md`
- [ ] `scripts/demo.ps1` — start semuanya berurutan untuk hari-H; latihan demo 15 menit sesuai §7 desain
- [ ] Screenshot untuk laporan: Spark UI (streaming + DAG batch), `hdfs dfs -ls`, mongosh, dashboard (peta live, historis, alert)
- [ ] Slide presentasi: masalah → arsitektur (diagram §3) → justifikasi platform (§5) → demo live → hasil chaos → keterbatasan & jalur produksi (§8)
- [ ] Finalisasi dokumen desain sebagai laporan (tambah screenshot + hasil eksperimen)

---

## Pembagian Kerja Ringkas

| Orang | Track | Deliverable | Bisa mulai setelah |
|---|---|---|---|
| 1 | A | infra jalan, `ingest.py`, `replay.py` | Track 0 |
| 2 | B | `streaming_job.py`, `batch_job.py`, `common.py` | Track 0 (pakai fixtures) |
| 3 | C | FastAPI REST + WebSocket | Track 0 (pakai seed) |
| 4 | D | React dashboard | Track 0 (pakai mock WS) |
| Semua | M1–M4 | integrasi, chaos, demo, slide | track masing² selesai |

Estimasi: Track 0 = ½ hari bersama; Track A–D = 2–4 hari paralel; M1–M4 = 2–3 hari (M1 butuh data mengendap ≥1 hari untuk batch yang meyakinkan — **mulai ingest riil sedini mungkin begitu A.2 jadi**).

# Monitoring Lalu Lintas Udara Indonesia — Big Data Pipeline

Pipeline end-to-end (pola Lambda) untuk monitoring lalu lintas udara Indonesia secara real-time, dibangun dari data publik **OpenSky Network**. Tugas mata kuliah: desain infrastruktur Big Data untuk permasalahan interdisipliner (logistik, smart city, lingkungan).

Dokumen desain arsitektur lengkap: [`docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md`](docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md)
Kontrak data antar komponen: [`docs/design/CONTRACTS.md`](docs/design/CONTRACTS.md)
Rencana implementasi: [`docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md`](docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md)

## Arsitektur singkat

```
OpenSky API ─► ingest.py ─┬─► HDFS raw zone (arsip)
                           └─► Kafka topic opensky.states (KRaft, key=icao24)
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                              ▼
          streaming_job.py                  batch_job.py
    (Spark Structured Streaming)             (PySpark, harian)
                    │                              │
                    ▼                              ▼
              MongoDB (live_states,          HDFS curated (Parquet)
              zone_stats, alerts)             + daily_snapshot → Mongo
                    │                              │
                    └──────────► api/ (FastAPI) ◄──┘
                       WebSocket /ws/live · REST /api/history/*
                                   │
                                   ▼
                        web/ (React + deck.gl/MapLibre)
```

> Transport landing sebelumnya folder lokal `landing_stream/` (NDJSON), dimigrasikan ke Kafka
> 2026-07-12 — lihat [`docs/design/2026-07-12-kafka-migration-design.md`](docs/design/2026-07-12-kafka-migration-design.md).

## Struktur repo

```
src/            # ingest.py, replay.py, batch_job.py, streaming_job.py, common.py
api/            # FastAPI serving layer
web/            # React SPA (Vite + deck.gl/MapLibre)
config/         # config.yaml (lokal, tidak di-commit) + config.example.yaml
scripts/        # helper start/stop infra, eksperimen chaos, demo
fixtures/       # data contoh untuk dev tanpa perlu infra penuh jalan
docs/           # dokumen desain, kontrak, rencana implementasi, laporan
```

## Setup

1. Aktifkan venv bersama di root `#bigdata` (dipakai lintas project big data di mesin ini): `D:\Coding\#bigdata\venv\Scripts\activate` lalu `pip install -r requirements.txt`. Tidak ada `venv/` lokal di folder `it-infra`.
2. Salin `config/config.example.yaml` → `config/config.yaml`, isi kredensial OpenSky (`opensky-network.org` → API client)
3. Pastikan Hadoop/HDFS, YARN, Kafka, & MongoDB (replica set 1 node) sudah jalan — lihat `scripts/start_infra.ps1` untuk cara otomatis, atau §"Menjalankan manual (step-by-step)" di bawah untuk belajar alurnya satu per satu
4. Jalankan komponen sesuai kebutuhan (lihat masing-masing skrip di `src/`, `api/`, `web/`)

Cara tercepat untuk semuanya sekaligus: `scripts\run_backend.cmd` (atau `.ps1` dari PowerShell) — lihat isi script untuk detail tiap step. Bagian di bawah ini untuk yang mau jalankan **manual satu per satu** (mis. untuk belajar alurnya, atau debugging komponen tertentu).

## Menjalankan manual (step-by-step)

Precondition: sudah pernah setup HDFS (`start-dfs.cmd`), YARN (`start-yarn.cmd`), MongoDB replica
set `rs0`, dan Kafka (format storage KRaft sekali — lihat [`docs/plans/2026-07-12-kafka-migration.md`](docs/plans/2026-07-12-kafka-migration.md) Task 1). Urutan di bawah untuk **menyalakan** service/komponen yang sudah pernah di-setup, bukan setup dari nol.

### 1. HDFS + YARN

```
start-dfs.cmd
start-yarn.cmd
```

Cek semua proses sudah hidup:

```
jps
```

Harus muncul `NameNode`, `DataNode`, `ResourceManager`, `NodeManager` (proses JVM lain seperti `AppKt` tidak terkait project ini — `jps` menampilkan semua proses JVM di mesin).

### 2. MongoDB — cek replica set jalan

MongoDB bukan proses JVM, tidak muncul di `jps`. Cek pakai `mongosh`:

```
mongosh "mongodb://localhost:27017/?replicaSet=rs0"
```

Di dalam shell:

```js
use opensky
db.live_states.countDocuments()
db.zone_stats.countDocuments()
db.alerts.countDocuments()
```

Untuk pantau upsert masuk **real-time** (berguna sambil nge-tes `streaming_job.py` di step 6) — pakai change stream, `cursor.next()` blocking sampai ada event baru (bukan `.on('change', ...)`, itu API driver Node.js biasa, tidak didukung `mongosh`):

```js
const cs = db.watch([], {fullDocument: "updateLookup"})
while (true) { printjson(cs.next()) }
```

Biarkan jendela ini terbuka terpisah selama development — event upsert akan otomatis ter-print begitu masuk.

### 3. Kafka broker

```
kafka-server-start.bat %KAFKA_HOME%\config\kraft\server.properties
```

(Butuh `KAFKA_HOME` & `%KAFKA_HOME%\bin\windows` sudah ada di PATH — lihat System Environment Variables. Broker harus sudah pernah diformat sekali sebelumnya, `kafka-storage.bat format`, tidak perlu diulang tiap start.)

Cek topic `opensky.states` ada:

```
kafka-topics.bat --list --bootstrap-server localhost:9092
```

Pantau pesan masuk real-time (opsional, mirip fungsi `mongosh watch()` di atas tapi level Kafka):

```
kafka-console-consumer.bat --topic opensky.states --bootstrap-server localhost:9092
```

### 4. API (FastAPI/uvicorn)

```
cd D:\Coding\#bigdata\it-infra
D:\Coding\#bigdata\venv\Scripts\activate
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

**Jangan** tambahkan `--reload` — di Windows itu bikin `/api/history/refresh` selalu gagal (worker `--reload` jalan di bawah `SelectorEventLoop`, yang tidak bisa spawn subprocess ke `spark-submit.cmd` sama sekali).

### 5. ingest.py (poller OpenSky)

Jendela terminal baru, venv sudah di-activate:

```
python src\ingest.py
```

Publish raw JSON ke HDFS `raw/dt=<tanggal>/` (arsip) **dan** ke Kafka topic `opensky.states` (dikonsumsi step 6). Tanpa `streaming_job.py` jalan, data numpuk di Kafka tapi MongoDB tetap kosong — itu normal, bukan bug.

### 6. streaming_job.py (Spark Structured Streaming)

Jendela terminal baru. **Wajib** set env var `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` **sebelum** memanggil `spark-submit` (kalau di-set dari dalam file `.py`, driver Spark keburu start pakai `python` polos dari PATH duluan):

```
set PYSPARK_PYTHON=D:\Coding\#bigdata\venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=D:\Coding\#bigdata\venv\Scripts\python.exe
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src\streaming_job.py
```

`--packages` wajib (connector Kafka bukan bagian default Spark) — didownload via Ivy sekali (butuh internet), lalu ter-cache lokal untuk run berikutnya. Setelah beberapa saat, cek `http://localhost:4040` — **Spark UI baru muncul setelah `spark-submit` benar-benar jalan** (HDFS/YARN infra saja tidak memunculkan 4040, itu normal).

### 7. batch_job.py (opsional, manual per tanggal)

```
spark-submit src\batch_job.py --date 2026-07-12
```

(Env var `PYSPARK_*` dari step 6 di jendela yang sama masih berlaku kalau dijalankan di window yang sama; kalau window baru, set ulang seperti step 6.)

### 8. Frontend (opsional)

```
cd D:\Coding\Projects\it-infra-web
npm run dev
```

## Status implementasi

Lihat checklist di [`docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md`](docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md).

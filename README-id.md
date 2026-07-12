# Monitoring Lalu Lintas Udara Indonesia — Big Data Pipeline

*Read this in [English](README.md).*

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

## Prasyarat

Infrastruktur di-install manual (bukan via package manager/container), versi yang teruji jalan bersama:

| Komponen | Versi teruji | Catatan |
|---|---|---|
| Java (JDK) | 8 (1.8.0_471) | Wajib JDK 8 atau 11 untuk Hadoop 3.3.x — JDK 17+ belum didukung penuh |
| Hadoop | 3.3.6 | HDFS + YARN, single-node (pseudo-distributed) |
| Spark | 3.5.1 (bin-hadoop3) | `spark-submit` dipakai untuk `streaming_job.py` & `batch_job.py` |
| Kafka | 3.9.2 (Scala 2.13) | Mode KRaft (tanpa ZooKeeper) |
| MongoDB | 8.0, replica set 1-node (`rs0`) | Change stream dipakai `api/main.py` untuk `/ws/live` |
| Python | 3.11 | Lihat `requirements.txt` |
| Node.js | 22.x | Untuk `web/` (frontend, repo terpisah `it-infra-web`) |

Semua tool di atas **tidak wajib** ada di path yang sama seperti mesin developer — lihat bagian berikut.

## Konfigurasi path lokal (supaya bisa jalan di mesin siapa pun)

Repo ini tidak hardcode lokasi instalasi Hadoop/Spark/Kafka/venv di mesin manapun. Yang perlu Anda siapkan di mesin sendiri:

1. **`JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME`, `KAFKA_HOME`** — System Environment Variables standar Windows, isi sesuai ke mana Anda meng-install masing-masing tool (lihat tabel Prasyarat untuk versi). Semua script (`scripts/*.ps1`) dan command manual di README ini membaca variable ini, bukan path literal — jadi bebas mau di-install di drive/folder apa saja.
2. **Venv Python** — dua opsi:
   - **Cukup buat venv sendiri di dalam repo ini** (paling sederhana untuk kontributor baru):
     ```
     python -m venv venv
     venv\Scripts\activate
     pip install -r requirements.txt
     ```
     Semua script di `scripts/` otomatis mendeteksi `venv\Scripts\python.exe` di root repo sebagai default kalau tidak ada override.
   - **Pakai satu venv bersama di lokasi lain** (mis. dipakai lintas beberapa project big data sekaligus) — set env var `BIGDATA_VENV_PYTHON` ke path `python.exe` venv tersebut, atau cukup `activate` venv itu sebelum menjalankan script (script mendeteksi `VIRTUAL_ENV` juga). Urutan resolusi di semua script: `BIGDATA_VENV_PYTHON` → venv aktif (`VIRTUAL_ENV`) → fallback `venv\Scripts\python.exe` di root repo.
3. **`config/config.yaml`** — copy dari `config/config.example.yaml`, isi kredensial OpenSky sendiri (lihat §Setup).

## Setup

1. Siapkan venv (lihat §Konfigurasi path lokal di atas), lalu `pip install -r requirements.txt`.
2. Salin `config/config.example.yaml` → `config/config.yaml`, isi kredensial OpenSky (daftar dulu di `opensky-network.org` → buat API client OAuth2).
3. Pastikan Hadoop/HDFS, YARN, Kafka, & MongoDB (replica set 1 node) sudah ter-install dan `JAVA_HOME`/`HADOOP_HOME`/`SPARK_HOME`/`KAFKA_HOME` sudah di-set — lihat §Prasyarat & §Konfigurasi path lokal.
4. Jalankan komponen sesuai kebutuhan — lihat §Menjalankan otomatis atau §Menjalankan manual di bawah.

## Menjalankan otomatis

Cara tercepat untuk semua komponen sekaligus:

```
scripts\run_backend.cmd
```

(atau `scripts\run_backend.ps1` langsung dari PowerShell). Script ini akan:

1. Start HDFS + cek MongoDB (`start_infra.ps1`)
2. Start Kafka broker
3. Start API (`uvicorn`, port 8000)
4. Start `ingest.py` (poller OpenSky)
5. Start `streaming_job.py` (Spark Structured Streaming)
6. Jalankan `batch_job.py` sekali untuk tanggal hari ini (supaya tab History langsung ada data)

Setiap komponen jangka-panjang dibuka di window terpisah supaya log masing-masing kelihatan. Prasyarat sebelum pakai script ini:

- Venv sudah ada (lihat §Konfigurasi path lokal) — script akan error dengan pesan jelas kalau tidak ketemu.
- `KAFKA_HOME` sudah di-set dan broker **sudah pernah diformat sekali** (`kafka-storage.bat format` — lihat [`docs/plans/2026-07-12-kafka-migration.md`](docs/plans/2026-07-12-kafka-migration.md) Task 1, sekali saja per instalasi).
- HDFS namenode sudah pernah diformat, dan MongoDB replica set `rs0` sudah pernah di-`rs.initiate()` — sekali saja per instalasi, lihat §Menjalankan manual langkah 1–2 untuk detail kalau belum pernah setup.

Yang **tidak** dijalankan otomatis (jalankan manual kalau perlu):

- `src\replay.py` — alternatif `ingest.py` offline pakai data rekaman (bukan dipakai bersamaan dengan `ingest.py`)
- `web/` (frontend) — `npm run dev` di repo `it-infra-web` (terpisah)

Script pendukung lain di `scripts/`:

| Script | Fungsi |
|---|---|
| `start_infra.ps1` | Hanya start HDFS + cek MongoDB (dipanggil `run_backend.ps1`, bisa dipanggil sendiri) |
| `run_batch_daily.ps1 -Date YYYY-MM-DD` | Jalankan `batch_job.py` untuk tanggal tertentu (default kemarin); bisa didaftarkan ke Task Scheduler |
| `run_yarn_density_grid.ps1 -Date YYYY-MM-DD` | Job MapReduce (Hadoop Streaming) alternatif Spark-on-YARN, lihat komentar di file untuk konteks |
| `demo.ps1` | Skrip pemandu presentasi/demo end-to-end (~15 menit), tidak menjalankan komponen sendiri |

## Menjalankan manual (step-by-step)

Precondition: sudah pernah setup HDFS (`start-dfs.cmd`), YARN (`start-yarn.cmd`), MongoDB replica
set `rs0`, dan Kafka (format storage KRaft sekali — lihat [`docs/plans/2026-07-12-kafka-migration.md`](docs/plans/2026-07-12-kafka-migration.md) Task 1). Urutan di bawah untuk **menyalakan** service/komponen yang sudah pernah di-setup, bukan setup dari nol.

Contoh command di bawah asumsikan venv di root repo (`venv\Scripts\...`) — ganti dengan lokasi venv Anda kalau pakai opsi `BIGDATA_VENV_PYTHON`/venv terpisah (lihat §Konfigurasi path lokal).

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

(Butuh `KAFKA_HOME` & `%KAFKA_HOME%\bin\windows` sudah ada di PATH — lihat §Konfigurasi path lokal. Broker harus sudah pernah diformat sekali sebelumnya, `kafka-storage.bat format`, tidak perlu diulang tiap start.)

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
cd <path-repo-Anda>
venv\Scripts\activate
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

**Jangan** tambahkan `--reload` — di Windows itu bikin `/api/history/refresh` selalu gagal (worker `--reload` jalan di bawah `SelectorEventLoop`, yang tidak bisa spawn subprocess ke `spark-submit.cmd` sama sekali).

### 5. ingest.py (poller OpenSky)

Jendela terminal baru — **wajib** berada di root repo ini (path `src\...` di bawah relatif ke situ):

```
cd <path-repo-Anda>
venv\Scripts\activate
python src\ingest.py
```

Publish raw JSON ke HDFS `raw/dt=<tanggal>/` (arsip) **dan** ke Kafka topic `opensky.states` (dikonsumsi step 6). Tanpa `streaming_job.py` jalan, data numpuk di Kafka tapi MongoDB tetap kosong — itu normal, bukan bug.

### 6. streaming_job.py (Spark Structured Streaming)

Jendela terminal baru — **wajib** berada di root repo ini, karena path `src\streaming_job.py` di bawah relatif ke situ. **Wajib** juga set env var `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` **sebelum** memanggil `spark-submit` (kalau di-set dari dalam file `.py`, driver Spark keburu start pakai `python` polos dari PATH duluan):

```
cd <path-repo-Anda>
set PYSPARK_PYTHON=<path-repo-Anda>\venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=<path-repo-Anda>\venv\Scripts\python.exe
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src\streaming_job.py
```

`--packages` wajib (connector Kafka bukan bagian default Spark) — didownload via Ivy sekali (butuh internet), lalu ter-cache lokal untuk run berikutnya. Setelah beberapa saat, cek `http://localhost:4040` — **Spark UI baru muncul setelah `spark-submit` benar-benar jalan** (HDFS/YARN infra saja tidak memunculkan 4040, itu normal).

### 7. batch_job.py (opsional, manual per tanggal)

Sama seperti step 6 — jalankan dari root repo ini:

```
cd <path-repo-Anda>
spark-submit src\batch_job.py --date 2026-07-12
```

(Env var `PYSPARK_*` dari step 6 di jendela yang sama masih berlaku kalau dijalankan di window yang sama; kalau window baru, set ulang seperti step 6.)

### 8. Frontend (opsional)

```
cd <path-repo-it-infra-web-Anda>
npm run dev
```

## Status implementasi

Lihat checklist di [`docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md`](docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md).

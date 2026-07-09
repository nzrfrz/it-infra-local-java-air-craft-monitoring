# Desain Infrastruktur Big Data: Monitoring Lalu Lintas Udara Indonesia Real-Time

**Status:** Draft untuk review
**Tanggal:** 8 Juli 2026
**Konteks:** Tugas mata kuliah — desain infrastruktur Big Data end-to-end untuk permasalahan interdisipliner.

---

## 1. Latar Belakang & Rumusan Masalah

Lalu lintas udara adalah permasalahan interdisipliner: **logistik** (efisiensi rute, kepadatan bandara, konektivitas antar pulau — vital bagi negara kepulauan), **smart city** (perencanaan kapasitas bandara, kebisingan wilayah urban), dan **lingkungan** (estimasi emisi dari kepadatan penerbangan per wilayah). Indonesia memiliki salah satu ruang udara tersibuk di Asia Tenggara, dengan CGK (Soekarno-Hatta) melayani 1.000+ pergerakan pesawat per hari.

**Pertanyaan yang dijawab sistem ini:**

1. *Real-time:* Berapa jumlah dan kepadatan pesawat di ruang udara Indonesia saat ini per zona? Adakah kondisi darurat (squawk 7500/7600/7700) atau anomali kepadatan? → **jalur streaming**
2. *Historis:* Bagaimana pola kepadatan lalu lintas udara per jam/hari? Jam-jam tersibuk di sekitar bandara utama? Distribusi ketinggian & kecepatan? Maskapai/negara asal dominan? → **jalur batch**

Dua kebutuhan latensi yang berbeda (detik-menit vs jam-hari) inilah yang menjustifikasi arsitektur dengan dua jalur pemrosesan.

## 2. Sumber Data (Akuisisi Tanpa IoT Fisik)

Sesuai ketentuan tugas, tidak ada sensor fisik milik sendiri. Data diambil dari **OpenSky Network** — jaringan crowdsourced ribuan receiver ADS-B di seluruh dunia yang menangkap sinyal transponder pesawat. Secara konseptual ini tetap data sensor/IoT (transponder pesawat = sensor bergerak), hanya diakses lewat lapisan API — sesuai ketentuan "ambil dari API publik atau sejenis".

| Sumber | Data | Frekuensi update | Autentikasi | Format |
|---|---|---|---|---|
| **OpenSky Network REST API** (`/api/states/all`) | State vector semua pesawat dalam bounding box: posisi, ketinggian, kecepatan, heading, squawk | **5–10 detik** di sisi sumber; polling kita tiap 30–60 dtk | Akun gratis (OAuth2 client credentials) | JSON |

### Manajemen kuota API (keputusan desain penting)

OpenSky memakai sistem kredit harian: akun terdaftar mendapat **4.000 kredit/hari**, dan biaya per panggilan tergantung luas bounding box (≤25 deg² = 1 kredit; 25–100 = 2; 100–400 = 3; >400 = 4).

- Bounding box **seluruh Indonesia** (lat -11°…6°, lon 95°…141° ≈ 780 deg²) = 4 kredit/panggilan → maksimal 1.000 panggilan/hari ≈ 1 panggilan per 86 detik.
- Bounding box **Jawa & sekitarnya** (lat -9°…-5°, lon 105°…115° ≈ 40 deg²) = 2 kredit → 2.000 panggilan/hari ≈ 1 panggilan per 43 detik.

**Keputusan:** ingestor memakai **dua tier polling** — bounding box Jawa (mencakup CGK, HLP, SUB, JOG, BDO; wilayah tersibuk) di-poll tiap **60 detik** (1.440 × 2 = 2.880 kredit), dan snapshot seluruh Indonesia diambil tiap **15 menit** untuk konteks nasional (96 × 4 = 384 kredit). Total ≈ **3.264 dari 4.000 kredit/hari** — menyisakan ~18% buffer untuk retry dan pengujian manual saat development. Interval 60 detik tetap bermakna secara data: pesawat jelajah bergerak ~15 km/menit, sehingga tiap snapshot menghasilkan posisi yang berbeda signifikan. Ini contoh nyata trade-off akuisisi data yang didokumentasikan sebagai justifikasi desain.

### Mode replay (fallback demo offline)

Ingestor menyimpan semua respons mentah; skrip `replay.py` dapat memutar ulang rekaman ke folder landing streaming dengan kecepatan aslinya (atau dipercepat). Berguna bila demo berlangsung saat koneksi internet bermasalah atau kuota habis — dan sesuai ketentuan tugas yang membolehkan data disimulasikan.

**Volume data:** ±50–150 pesawat per snapshot Jawa × ~1.440 snapshot/hari ≈ **70.000–220.000 record/hari** — cukup untuk merasakan karakter big data pada prototipe.

## 3. Arsitektur Keseluruhan (Pola Lambda)

Arsitektur mengikuti **pola Lambda**: *batch layer* untuk akurasi & analisis mendalam atas seluruh data historis, *speed layer* untuk hasil real-time berlatensi rendah, dan *serving layer* untuk konsumsi dashboard.

```
┌─────────────────────────────────────────────────────────────────────┐
│ AKUISISI                                                            │
│  OpenSky API ──► ingest.py (poller 2-tier: Jawa/60dtk, ID/15mnt)    │
│                        │                                            │
│                        ├──► HDFS /data/raw/opensky/{dt}/*.json      │
│                        └──► landing_stream/ (file JSON kecil per    │
│                             snapshot utk dikonsumsi streaming)      │
│  (replay.py = fallback memutar ulang rekaman ke landing_stream/)    │
├─────────────────────────────────────────────────────────────────────┤
│ PENYIMPANAN                                                         │
│  HDFS  = data lake (raw zone + curated zone, format Parquet)        │
│  MongoDB = serving layer NoSQL (state real-time + alert + agregat)  │
├─────────────────────────────────────────────────────────────────────┤
│ PEMROSESAN                                                          │
│  BATCH  : batch_job.py (PySpark, dijadwalkan harian)                │
│           raw JSON → flatten state vectors → dedup → agregasi:      │
│           kepadatan per grid 0.5°×0.5° per jam, jam tersibuk per    │
│           bandara, distribusi altitude/velocity, top negara asal &  │
│           prefix callsign → Parquet di HDFS curated                 │
│           + snapshot ringkasan harian ke MongoDB                    │
│  STREAM : streaming_job.py (Spark Structured Streaming)             │
│           file source (landing_stream/) → parsing → windowed agg    │
│           (jumlah pesawat & rata2 kecepatan per zona per window     │
│           2 mnt, watermark 1 mnt) → deteksi alert:                  │
│           • squawk darurat 7500/7600/7700                           │
│           • lonjakan kepadatan zona (> μ+3σ baseline)               │
│           → sink MongoDB; checkpoint → HDFS (fault tolerance)       │
├─────────────────────────────────────────────────────────────────────┤
│ SERVING API                                                         │
│  api/ (FastAPI)                                                     │
│   • WebSocket /ws/live   : forward MongoDB change stream            │
│     (live_states, zone_stats, alerts) → push realtime ke frontend   │
│   • REST /api/history/*  : baca Parquet curated (heatmap, tren,     │
│     distribusi) — hasil di-cache per hari                           │
├─────────────────────────────────────────────────────────────────────┤
│ VISUALISASI                                                         │
│  web/ (React SPA — Vite + deck.gl/MapLibre)                         │
│   • Peta live      : marker pesawat ter-update via WebSocket push,  │
│                      tanpa refresh halaman                          │
│   • Panel historis : heatmap kepadatan, tren per jam, distribusi    │
│                      altitude — dari REST API                       │
│   • Panel alert    : squawk darurat & anomali via WebSocket         │
└─────────────────────────────────────────────────────────────────────┘
```

### Alur data ringkas

1. `ingest.py` polling OpenSky → menulis JSON mentah ke **HDFS raw zone** (arsip permanen, bisa diproses ulang) dan file kecil per-snapshot ke **folder landing streaming**.
2. **Jalur batch** (harian): Spark membaca seluruh raw zone → flatten array state vector menjadi baris per pesawat-per-waktu → dedup `(icao24, time_position)` → validasi (koordinat dalam bbox, altitude masuk akal) → agregasi kepadatan, pola temporal, statistik → Parquet terpartisi `dt/` ke curated zone.
3. **Jalur streaming** (kontinu): Structured Streaming memantau folder landing → agregasi berjendela 2 menit per zona dengan watermark 1 menit → flag alert → upsert ke MongoDB.
4. **Serving API** (FastAPI) menjembatani penyimpanan dan frontend: MongoDB **change stream** di-forward via WebSocket (begitu streaming job menulis, frontend langsung menerima push — tanpa polling), dan data historis Parquet disajikan lewat endpoint REST.
5. **Dashboard React** menerima push WebSocket untuk peta live & alert (tanpa refresh halaman) dan memanggil REST untuk panel historis.

## 4. Desain Skema Data

### HDFS — struktur direktori

```
/bigdata/opensky/
├── raw/
│   └── dt=2026-07-08/snapshot_java_20260708T101530.json
├── curated/
│   ├── states_clean/dt=.../part-*.parquet        # 1 baris = 1 pesawat @ 1 waktu
│   ├── density_grid_hourly/dt=.../part-*.parquet
│   ├── airport_hourly/dt=.../part-*.parquet
│   └── daily_summary/dt=.../part-*.parquet
└── checkpoints/streaming_opensky/
```

Raw disimpan apa adanya (schema-on-read) agar bisa diproses ulang; curated pakai Parquet (kolumnar, terkompresi, predicate pushdown untuk Spark).

### Skema curated `states_clean` (inti analisis batch)

| Kolom | Tipe | Keterangan |
|---|---|---|
| icao24 | string | ID unik transponder pesawat |
| callsign | string | nomor penerbangan (prefix = maskapai, mis. GIA, LNI, BTK) |
| origin_country | string | negara registrasi |
| ts | timestamp | waktu posisi |
| lat, lon | double | posisi |
| baro_altitude_m | double | ketinggian barometrik |
| velocity_ms | double | kecepatan |
| true_track | double | heading (derajat) |
| vertical_rate | double | laju naik/turun (proxy fase terbang) |
| on_ground | boolean | di darat/terbang |
| squawk | string | kode transponder (7500/7600/7700 = darurat) |
| grid_cell | string | sel grid 0.5°×0.5° (turunan lat/lon, untuk agregasi) |

### MongoDB — koleksi

| Koleksi | Isi | Ditulis oleh | Pola akses |
|---|---|---|---|
| `live_states` | posisi terkini per pesawat (upsert by `icao24`, TTL index 1 jam) | streaming_job | change stream → WebSocket push ke peta live |
| `zone_stats` | jumlah pesawat & rata2 kecepatan per zona per window 2 mnt (TTL 24 jam) | streaming_job | change stream → grafik real-time |
| `alerts` | squawk darurat + anomali kepadatan (icao24/zona, ts, jenis, severity) | streaming_job | change stream → panel alert |
| `daily_snapshot` | ringkasan harian (total pergerakan, jam tersibuk, top maskapai) | batch_job | REST, kartu ringkasan dashboard |

> Catatan: MongoDB change stream membutuhkan replica set; pada single-node cukup dijalankan sebagai replica set beranggota satu (`rs.initiate()`) — konfigurasi standar untuk development.

## 5. Justifikasi Pemilihan Platform

| Kebutuhan | Pilihan | Alternatif yang dipertimbangkan | Alasan memilih |
|---|---|---|---|
| Sumber data | **OpenSky Network** | ADS-B Exchange, FlightAware, AviationStack | Satu-satunya yang gratis dengan data posisi real-time (5–10 dtk) tanpa biaya; alternatif lain berbayar atau delay 5+ menit |
| Data lake | **HDFS** | Filesystem lokal, MinIO/S3 | Replikasi & fault tolerance bawaan, data locality dengan Spark, sudah terinstall (ketentuan tugas), standar de-facto ekosistem batch Hadoop |
| Format curated | **Parquet** | CSV, JSON, Avro | Kolumnar → scan analitik cepat, kompresi baik (penting: ratusan ribu baris/hari), schema evolution, dukungan native Spark |
| NoSQL serving | **MongoDB** | HBase, Cassandra, Redis | Dokumen JSON cocok dengan payload API, upsert + TTL index native (pas untuk state live yang kedaluwarsa), konektor Spark resmi, jauh lebih mudah dioperasikan di Windows daripada HBase; HBase unggul jika throughput tulis masif — tidak relevan pada skala ini |
| Batch | **Spark (PySpark)** | Hadoop MapReduce, pandas | In-memory → jauh lebih cepat dari MapReduce, API DataFrame ekspresif; pandas tidak scale-out |
| Streaming | **Spark Structured Streaming** | Flink, Kafka Streams | Satu engine & satu codebase dengan batch (menekan kompleksitas Lambda), exactly-once via checkpoint+WAL, sudah terinstall |
| Transport streaming | **File source** | Kafka | Pada prototipe single-node, Kafka menambah komponen tanpa nilai demonstratif; file source didukung resmi & tetap fault-tolerant. **Di produksi, Kafka menggantikan folder landing** (buffering, replay, multi-consumer) — lihat §8 |
| Serving API | **FastAPI** | Flask, Express (Node) | Async native (pas untuk WebSocket + change stream), satu bahasa dengan pipeline (akses pymongo/PyArrow langsung), dokumentasi OpenAPI otomatis |
| Dashboard | **React SPA (Vite) + deck.gl/MapLibre** | Streamlit, Next.js, Grafana | Update realtime via WebSocket push — marker pesawat bergerak tanpa refresh halaman (Streamlit selalu re-render per interval). Next.js dipertimbangkan namun ditolak: React Server Components dirender saat request — tidak membantu jalur realtime yang tetap butuh WebSocket di client, sementara menambah server Node di samping FastAPI. deck.gl efisien merender ratusan marker bergerak |
| Orkestrasi | **Windows Task Scheduler / skrip** | Airflow | Airflow overkill untuk 1 job harian di 1 mesin; disebut sebagai jalur produksi |

## 6. Fault Tolerance & Validasi Resiliensi

Mekanisme bawaan yang diandalkan:

- **HDFS replication** (single-node: `dfs.replication=1` — keterbatasan diakui; desain produksi ≥3)
- **Structured Streaming checkpointing**: offset & state disimpan ke direktori checkpoint di HDFS; query yang mati bisa restart tanpa kehilangan/duplikasi (exactly-once ke sink idempoten — upsert MongoDB berdasarkan kunci `icao24` / `(zona, window)` bersifat idempoten)
- **Idempotensi ingest**: nama file berbasis `(tier, timestamp snapshot)` → retry tidak menduplikasi data; dedup ulang di batch sebagai lapisan kedua
- **Toleransi kegagalan API**: retry dengan exponential backoff; jika OpenSky down, streaming tetap hidup (tidak ada file baru = tidak ada micro-batch, bukan error)
- **Spark task retry**: kegagalan task/executor di-retry otomatis oleh scheduler

Validasi lewat 3 eksperimen chaos (dijalankan di lab, blast radius = 0 pengguna eksternal):

| # | Eksperimen | Hipotesis | Metrik steady-state | Rollback |
|---|---|---|---|---|
| 1 | Kill DataNode saat batch job | Job selesai bila replikasi ≥2; pada replikasi 1 job gagal — didokumentasikan sebagai temuan | exit code 0, `hdfs fsck` 0 missing block | restart DataNode, verifikasi fsck |
| 2 | Sumber data lambat/error (inject sleep 5 dtk + 20% error di ingestor) | Streaming lanjut, latensi naik gracefully, tidak ada record hilang setelah pulih | `numInputRows`>0/batch, checkpoint maju | cabut injeksi, rekonsiliasi row count |
| 3 | Kill executor saat streaming | Maksimal 1 micro-batch tertunda; recovery dari checkpoint tanpa duplikat | `processedRowsPerSecond` stabil | restart query dari checkpoint; ukur MTTR |

Hasil tiap eksperimen dicatat: hipotesis terbukti/terbantah, waktu pemulihan, tindak lanjut.

## 7. Prototipe Implementasi

Struktur repo:

```
it-infra/
├── docs/design/           ← dokumen ini + diagram
├── src/
│   ├── ingest.py          # poller OpenSky 2-tier → HDFS & landing_stream/
│   ├── replay.py          # putar ulang rekaman (fallback demo offline)
│   ├── batch_job.py       # PySpark: raw → curated Parquet + snapshot MongoDB
│   └── streaming_job.py   # Structured Streaming: landing → windowed agg + alert → MongoDB
├── api/                   # FastAPI: WebSocket (change stream) + REST historis
│   └── main.py
├── web/                   # React SPA (Vite + deck.gl/MapLibre)
│   └── src/...
├── config/config.yaml     # bounding box, kredensial OpenSky, path HDFS, koneksi Mongo
├── scripts/               # start/stop helper, eksperimen chaos
└── requirements.txt       # pyspark, pymongo, requests, fastapi, uvicorn, pyarrow, pyyaml
```

**Skenario demo end-to-end (±15 menit):**

1. Start HDFS + MongoDB (replica set); jalankan `ingest.py` → tunjukkan file JSON masuk ke HDFS dan landing folder tiap ±60 detik.
2. Jalankan `streaming_job.py` → tunjukkan Spark UI tab Structured Streaming (input rate, batch duration) dan koleksi `live_states` terisi di MongoDB.
3. Jalankan `batch_job.py` atas data raw yang terkumpul → tunjukkan Parquet di `hdfs dfs -ls` dan DAG job di Spark UI.
4. Start FastAPI (`uvicorn api.main:app`) + buka dashboard React → peta pesawat live di atas Jawa ter-update via WebSocket **tanpa refresh halaman**, heatmap kepadatan historis terisi, panel alert menampilkan anomali (bila ada; bisa dipicu lewat replay rekaman yang mengandung squawk darurat).
5. (Opsional, nilai plus) Eksperimen chaos #3: kill executor, tunjukkan recovery dari checkpoint.

## 8. Keterbatasan & Desain Produksi

Prototipe berjalan single-node Windows; dokumen mengakui gap berikut beserta jalur produksinya:

| Aspek | Prototipe | Produksi |
|---|---|---|
| Akuisisi | Polling REST 45 dtk (kuota 4.000 kredit) | Feed ADS-B langsung / lisensi komersial FlightAware, receiver sendiri |
| Transport streaming | Folder file lokal | **Kafka** (buffering, replay, multi-consumer, backpressure) |
| Cluster | Single-node, replikasi 1 | Multi-node YARN/Kubernetes, replikasi 3, HA NameNode |
| Orkestrasi | Task Scheduler | **Airflow** (dependensi, retry, SLA, backfill) |
| Keamanan | Kredensial di config lokal | Kerberos/Ranger, TLS, secret manager |
| Monitoring | Spark UI manual | Prometheus + Grafana, alerting |
| Kualitas data | Validasi inline di job | Great Expectations / data contract di pipeline |

> **Catatan implementasi (Windows):** `hdfs.cmd` CLI di Hadoop-on-Windows memotong argumen path yang mengandung karakter `=` (partisi Hive-style seperti `dt=2026-07-09` menjadi `dt`). `src/ingest.py` karena itu menulis raw JSON ke HDFS lewat **WebHDFS REST API** (bukan `hdfs dfs -put`), yang tidak terpengaruh bug tersebut. Penulisan Parquet oleh Spark (batch/streaming) tidak terdampak karena dilakukan lewat Hadoop FileSystem API langsung, bukan CLI.

## 9. Ringkasan Pemenuhan Ketentuan Tugas

| Ketentuan | Dipenuhi oleh |
|---|---|
| Akuisisi data (tanpa IoT fisik) | OpenSky Network API — data transponder pesawat real-time (§2) |
| Penyimpanan HDFS | Data lake raw + curated (§3, §4) |
| Penyimpanan NoSQL | MongoDB serving layer (§4) |
| Pemrosesan batch Spark | `batch_job.py` — kepadatan, pola temporal, statistik (§3, §7) |
| Structured Streaming | `streaming_job.py` — windowed agg + alert darurat/anomali (§3, §7) |
| Visualisasi | Dashboard React (WebSocket realtime): peta live + historis + alert (§3, §7) |
| Dokumen desain arsitektur | Dokumen ini |
| Justifikasi platform | §5 (+ §8 jalur produksi) |
| Prototipe sederhana | §7, demo end-to-end |

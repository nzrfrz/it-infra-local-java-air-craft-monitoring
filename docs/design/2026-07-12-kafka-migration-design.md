# Desain: Migrasi landing_stream/ (folder lokal) → Apache Kafka

> Status: **direncanakan, belum dieksekusi**. Ditulis 2026-07-12 atas pertanyaan user "apa
> local folder itu bisa diganti Kafka?" — project M1-M4 sudah selesai sebelumnya, ini scope
> baru di luar rencana implementasi awal (`docs/plans/2026-07-08-...md`), murni pendalaman
> arsitektur atas inisiatif user.

## Kenapa

Landing zone saat ini (`landing_stream/`, folder lokal NDJSON, tmp+rename atomic) adalah
solusi murah untuk menjembatani `ingest.py` (producer) dan `streaming_job.py` (consumer, Spark
Structured Streaming file source). Ini bekerja, tapi Kafka adalah pola yang lebih representatif
untuk "message queue antara producer dan stream processor" di dunia nyata:

- Tidak ada masalah atomicity file kecil (folder lokal butuh trik tmp+rename manual; Kafka
  broker yang menjamin pesan utuh)
- Delivery semantics eksplisit (offset-based, bukan "file muncul di folder lalu dihapus siapa?"
  — sebetulnya di setup sekarang file lama di `landing_stream/` tidak pernah dibersihkan,
  numpuk terus)
- Replay/rewind bawaan (consumer bisa baca ulang dari offset tertentu — mirip fungsi
  `replay.py` sekarang, tapi generik di level infra, bukan script custom)
- Scaling ke banyak producer/consumer lebih natural (bukan relevan untuk scope tugas ini, tapi
  ini alasan Kafka dipilih di industri untuk kasus serupa)

**Bukan berarti setup sekarang salah** — untuk skala 1 laptop, folder lokal sudah cukup dan
sudah terbukti jalan (M1-M4 lolos semua). Ini murni exercise pendalaman, bukan bug fix.

## Yang TIDAK berubah

- `ingest.py` tetap menulis **raw JSON ke HDFS `raw/dt=<tanggal>/`** — arsip untuk `batch_job.py`,
  tidak terkait jalur streaming, tidak disentuh migrasi ini.
- `recordings/` (dipakai `replay.py` untuk demo offline) — tetap ada. `replay.py` sendiri perlu
  disesuaikan supaya bisa produce ke Kafka juga (lihat §Follow-up), tapi itu task terpisah,
  tidak wajib untuk membuktikan migrasi utama.
- Kontrak C1 (format NDJSON per baris) tidak berubah — cuma medium transportnya yang beda
  (file di disk → pesan Kafka). Isi payload JSON identik.
- MongoDB, API, frontend — sama sekali tidak terpengaruh. Kafka cuma menggantikan bagian
  "`ingest.py` → landing zone → `streaming_job.py`", bukan apa pun setelah itu.

## Yang berubah

```
SEBELUM:
ingest.py --tulis file--> landing_stream/*.json --readStream file source--> streaming_job.py

SESUDAH:
ingest.py --producer.send()--> Kafka topic "opensky.states" --readStream kafka source--> streaming_job.py
```

1. **`ingest.py`**: fungsi `_write_ndjson_atomic()` ke `landing_stream/` diganti `KafkaProducer.send()`
   per baris (atau per batch, lihat §Keputusan teknis). Dependency baru: `kafka-python`.
2. **`streaming_job.py`**: source `spark.readStream.format("json").load(landing_path)` diganti
   `spark.readStream.format("kafka").option("subscribe", "opensky.states")...`. Value Kafka
   (bytes) di-cast ke string lalu di-parse pakai `from_json` + `STATES_SCHEMA` yang sudah ada
   di `common.py` (skema tidak berubah). `spark-submit` perlu tambahan
   `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`.
3. **Checkpoint**: checkpoint Spark lama (`checkpoints/streaming_opensky/q1..q3`) berbasis file
   offset — tidak kompatibel dengan Kafka source (beda mekanisme offset tracking). Harus dihapus,
   query mulai fresh dari topic (pola yang sama seperti migrasi path landing dulu, lihat
   `what-have-done.md` §"Migrasi path data lokal").
4. **Infra baru**: Kafka broker (KRaft mode, tanpa Zookeeper — lihat §Keputusan teknis) jalan
   sebagai proses tambahan, mirip HDFS/YARN sekarang (dinyalakan manual atau lewat script).
5. **`config/config.yaml`**: tambah section `kafka:` (bootstrap servers, topic name), path
   `landing_stream` tidak lagi dipakai `streaming_job.py` (bisa dihapus dari config setelah
   migrasi terbukti stabil, tapi dibiarkan dulu untuk `replay.py` sampai itu juga dimigrasikan).

## Keputusan teknis

**Versi Kafka: 3.8.x atau 3.9.x (bukan 4.0+).** `JAVA_HOME` di mesin ini masih Java 8
(`jdk1.8.0_471`, dipakai juga oleh Hadoop/Spark — mengganti ini berisiko merusak setup yang
sudah jalan). Kafka 4.0 (rilis akhir 2024) **mensyaratkan minimum Java 11 untuk broker** dan
sudah drop dukungan Zookeeper sepenuhnya. Kafka 3.8/3.9 masih mendukung broker jalan di Java 8
(walau sudah deprecated warning) **dan** sudah punya KRaft mode production-ready (sejak 3.3) —
jadi tidak perlu Zookeeper sama sekali, cukup 1 proses `kafka-server-start`.

**Mode: KRaft, bukan Zookeeper.** Zookeeper adalah proses tambahan yang perlu dikelola
terpisah (mirip menambah 1 lagi "HDFS"-nya) — KRaft mode Kafka mengelola metadata cluster
sendiri tanpa Zookeeper, lebih sederhana untuk single-node dev setup begini, dan merupakan
arah resmi Kafka ke depan (Zookeeper mode sudah deprecated).

**1 topic (`opensky.states`), bukan per-tier.** Tier (java/national) sudah ada sebagai field
`tier` di dalam payload JSON — tidak perlu topic terpisah, `streaming_job.py` tetap bisa filter/
proses semua data sama seperti sekarang (baca semua, tier cuma metadata).

**Key partisi: `icao24`.** Supaya semua event untuk 1 pesawat yang sama selalu masuk partition
yang sama (ordering per-pesawat terjaga) — relevan untuk `live_states` (upsert per icao24, kalau
kepencar ke partition beda + diproses beda urutan bisa ada race pada data yang sama, walau
risikonya kecil karena `foreachBatch` sudah upsert idempotent by `_id`).

**Retention topic: pendek (mis. 1-6 jam)**, karena ini bukan sumber data historis (itu tugas
HDFS raw + `batch_job.py`) — Kafka topic di sini murni buffer transien antara ingest dan
streaming, sama fungsinya dengan `landing_stream/` yang lama, cuma bedanya di infra yang lebih
matang.

## Risiko / hal yang mungkin ditemukan (berdasarkan pengalaman project ini dengan Hadoop/Spark di Windows)

- **Kafka native Windows `.bat` scripts** historically punya bug path serupa dengan
  `hdfs.cmd`/`hadoop.cmd` (path panjang, spasi, karakter khusus) — belum tentu separah itu untuk
  Kafka (lebih simpel dari Hadoop), tapi perlu diuji, bukan diasumsikan mulus.
- **`kafka-storage.bat format`** (langkah wajib KRaft sebelum start pertama kali) perlu
  cluster ID unik — kalau lupa dijalankan, broker gagal start dengan error yang cukup jelas
  ("No `meta.properties`").
- **`spark-submit --packages`** akan men-download jar `spark-sql-kafka-0-10` + dependency-nya
  (termasuk `kafka-clients`) lewat Ivy saat pertama kali dipanggil — butuh koneksi internet aktif
  sekali di awal (di-cache lokal setelahnya di `~/.ivy2`).
- **Producer di `ingest.py` butuh error handling eksplisit** kalau broker Kafka mati/belum
  jalan — pola yang sama seperti `_put_to_hdfs()` sekarang (gagal kirim tidak boleh menghentikan
  seluruh `poll_once`, cukup di-log).

## Follow-up (di luar scope migrasi utama, dicatat supaya tidak lupa)

- `replay.py` saat ini memutar ulang rekaman ke `landing_stream/` — perlu diubah produce ke
  Kafka juga supaya tetap berguna untuk demo offline setelah migrasi.
- Setelah migrasi terbukti stabil beberapa hari, folder `landing_stream/` & referensinya di
  `config.yaml`/`CONTRACTS.md` §C1 bisa dibersihkan total (dokumen ini menandai "full replace",
  bukan dual-write permanen).

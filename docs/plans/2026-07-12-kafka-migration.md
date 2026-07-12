# Plan: Migrasi landing_stream/ → Kafka

> Desain lengkap & alasan keputusan: `docs/design/2026-07-12-kafka-migration-design.md`.
> Eksekusi solo sequential (konsisten dengan preferensi kerja project ini) — konfirmasi ke user
> sebelum lanjut task berikutnya. Instalasi Kafka (Task 1) dilakukan **manual oleh user**, bukan
> lewat script — task lain (kode) dieksekusi assistant setelah broker terbukti hidup.

## Task 0 — Prasyarat (cek dulu, jangan asumsi)

- [ ] Konfirmasi `JAVA_HOME` masih Java 8 (`jdk1.8.0_471`) — Kafka versi yang dipilih (3.8/3.9)
      harus kompatibel dengan ini, **jangan** install Java 11/17 terpisah kalau tidak perlu
      (risiko bentrok dengan Hadoop/Spark yang sudah jalan di Java 8).
- [ ] Pastikan port `9092` (Kafka broker default) belum dipakai proses lain.

## Task 1 — Install Kafka native Windows (MANUAL, dijalankan user sendiri)

1. Download **Kafka 3.9.x** (binary Scala 2.13, cocok dengan Spark yang pakai Scala 2.12 —
   Kafka client itu sendiri Scala-version-agnostic dari sisi Spark connector, jadi ini aman)
   dari `https://kafka.apache.org/downloads` — pilih tarball, ekstrak ke lokasi tanpa spasi/`#`
   di path (belajar dari pengalaman project ini dengan Hadoop — pakai mis. `C:\Kafka\kafka_2.13-3.9.x`,
   **hindari** `D:\Coding\#bigdata\...` untuk instalasi Kafka-nya sendiri karena karakter `#`
   sudah beberapa kali jadi sumber bug path di tool Windows lain di project ini).
2. Set environment variable (opsional tapi memudahkan) `KAFKA_HOME` ke folder ekstrak.
3. **Format storage KRaft** (wajib sekali sebelum start pertama kali):
   ```
   cd C:\Kafka\kafka_2.13-3.9.x
   .\bin\windows\kafka-storage.bat random-uuid
   ```
   Catat UUID yang dihasilkan, lalu:
   ```
   .\bin\windows\kafka-storage.bat format -t <UUID_TADI> -c .\config\kraft\server.properties
   ```
4. **Start broker** (foreground, jendela terpisah — sama pola dengan HDFS/YARN sekarang):
   ```
   .\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
   ```
   Tunggu sampai log menunjukkan broker siap (baris seperti `Kafka Server started`).
5. **Buat topic** `opensky.states` (jendela terminal baru, broker tetap jalan):
   ```
   .\bin\windows\kafka-topics.bat --create --topic opensky.states --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   ```
6. **Verifikasi manual** — kirim 1 pesan test lalu baca balik, pastikan broker benar-benar
   berfungsi sebelum lanjut ke integrasi kode:
   ```
   .\bin\windows\kafka-console-producer.bat --topic opensky.states --bootstrap-server localhost:9092
   ```
   (ketik `{"test":true}` lalu Enter, lalu Ctrl+C), buka jendela lain:
   ```
   .\bin\windows\kafka-console-consumer.bat --topic opensky.states --bootstrap-server localhost:9092 --from-beginning
   ```
   Harus muncul pesan test yang barusan dikirim.

**Checkpoint sebelum lanjut Task 2:** laporkan ke assistant kalau Task 1 sudah sukses (atau
kalau ketemu error — banyak kemungkinan bug Windows-spesifik seperti project ini biasa temui,
lebih baik didiagnosis bareng daripada dipaksa lanjut).

## Task 2 — `ingest.py`: producer Kafka menggantikan tulis ke `landing_stream/`

- [ ] Tambah `kafka-python` ke `requirements.txt`, install ke venv bersama
- [ ] Tambah section `kafka:` di `config/config.example.yaml` + `config/config.yaml`
      (`bootstrap_servers: "localhost:9092"`, `topic: "opensky.states"`)
- [ ] `write_outputs()`: ganti panggilan `_write_ndjson_atomic(flat_rows, landing_dir, filename)`
      jadi kirim tiap baris di `flat_rows` sebagai 1 pesan Kafka (`key=icao24.encode()`,
      `value=json.dumps(row).encode()`) ke topic `opensky.states`
- [ ] Producer dibuat sekali di awal `main()` (bukan per-poll), `acks="1"` cukup untuk use-case
      ini (bukan `acks="all"`, karena `replication-factor=1` — tidak ada replica lain untuk
      di-ack)
- [ ] Error handling: kegagalan kirim ke Kafka di-log (`log.error`), **tidak** menghentikan
      `poll_once` — pola sama dengan `_put_to_hdfs()` yang sudah ada
- [ ] `recordings/` (baris `_write_ndjson_atomic(flat_rows, recordings_dir, filename)`) **tetap
      dipertahankan apa adanya** — tidak terkait migrasi ini, masih dipakai `replay.py`

## Task 3 — `streaming_job.py`: Kafka source menggantikan file source

- [ ] Ganti `_as_file_uri(...)` + `spark.readStream.format("json").load(...)` jadi:
      ```python
      raw_stream = (
          spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
          .option("subscribe", cfg["kafka"]["topic"])
          .option("startingOffsets", "latest")
          .load()
      )
      base = (
          raw_stream
          .selectExpr("CAST(value AS STRING) as json_str")
          .select(from_json(col("json_str"), STATES_SCHEMA).alias("data"))
          .select("data.*")
      )
      ```
      (skema `STATES_SCHEMA` dari `common.py` tidak berubah — payload JSON per pesan identik
      dengan baris NDJSON lama)
- [ ] Hapus checkpoint lama sebelum run pertama: `checkpoints/streaming_opensky/q1_live_states`,
      `q2_zone_stats`, `q3_alerts` (checkpoint file-source tidak kompatibel dengan Kafka source
      — pola sama seperti migrasi path landing dulu)
- [ ] `spark-submit` perlu tambahan flag:
      ```
      spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src\streaming_job.py
      ```
      (jar didownload via Ivy sekali, butuh internet aktif saat pertama kali)

## Task 4 — Verifikasi end-to-end

- [ ] Start Kafka broker → `ingest.py` → `streaming_job.py` (dengan `--packages`), urutan sama
      seperti sesi manual start yang sedang berjalan sekarang, cuma broker Kafka gantikan
      "cek folder landing_stream"
- [ ] Pantau lewat `kafka-console-consumer.bat` bahwa `ingest.py` benar-benar publish pesan
- [ ] Pantau lewat `mongosh` `db.watch()` (pola yang sudah dipakai sesi ini) bahwa
      `live_states`/`zone_stats`/`alerts` tetap ter-upsert seperti sebelumnya
- [ ] Bandingkan angka dengan baseline sebelum migrasi (jumlah live_states, sebaran zona) —
      pastikan tidak ada regresi data (mis. semua pesan hilang karena salah `startingOffsets`,
      atau duplikat karena replay offset salah)

## Task 5 — Bersih-bersih & dokumentasi (setelah Task 4 stabil)

- [ ] Update `docs/design/CONTRACTS.md` §C1 — catat bahwa transport landing sekarang Kafka topic
      `opensky.states`, format payload (NDJSON per baris) tidak berubah
- [ ] Update `docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md` diagram
      §3 (akuisisi) — ganti "landing_stream/" jadi "Kafka topic opensky.states"
- [ ] Update `scripts/run_backend.ps1`/`.cmd` — tambah step start Kafka broker, ubah command
      `streaming_job.py` untuk include `--packages`
- [ ] `what-have-done.md` — catat sesi migrasi ini
- [ ] (Opsional, follow-up terpisah — lihat design doc) migrasikan `replay.py` supaya produce
      ke Kafka juga, bukan lagi tulis ke `landing_stream/`

---

**Titik mulai:** Task 1, dijalankan manual oleh user. Assistant menunggu konfirmasi sebelum
mulai Task 2 (perubahan kode).

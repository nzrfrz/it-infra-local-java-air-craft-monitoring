# Panduan Manual — M4 Validasi Resiliensi

Panduan langkah-demi-langkah untuk menjalankan M4 (3 eksperimen chaos + persiapan presentasi) **secara manual**. Dokumen ini berisi CARA-nya saja — hasil eksekusi (angka, screenshot, temuan spesifik) dicatat terpisah di folder `reports/`, bukan di sini.

Rencana kerja versi lengkap (checklist untuk tracking task-by-task) ada di `docs/plans/2026-07-12-m4-validasi-resiliensi.md`. Panduan ini adalah versi ringkas yang fokus ke prosedur teknisnya saja.

> **Update 2026-07-12 (pasca migrasi Kafka):** transport streaming sekarang Kafka topic `opensky.states`, bukan lagi folder `landing_stream/` — lihat `docs/design/2026-07-12-kafka-migration-design.md`. Semua command venv di bawah pakai path portable (`venv\Scripts\...`), bukan lagi hardcode `D:\Coding\#bigdata\venv` — kalau Anda pakai venv bersama lintas project, set `BIGDATA_VENV_PYTHON` dulu (lihat `README.md` §Konfigurasi path lokal). Ganti `<venv>` di bawah dengan path venv Anda kalau tidak pakai default self-contained di root repo.

## Prasyarat

- Backend jalan penuh: HDFS (NameNode+DataNode), MongoDB, Kafka broker, API, `ingest.py`, `streaming_job.py` — cek lewat `jps` (harus ada `NameNode`, `DataNode`, `Kafka`, `SparkSubmit`) dan `Get-Service MongoDB`. Cara tercepat: `scripts\run_backend.cmd`.
- Sudah ada data yang mengendap minimal beberapa hari (supaya batch job punya sesuatu untuk diproses).

## Task 0 — Smoke-check M1-M3

Tujuan: pastikan streaming, serving, dan batch semuanya masih jalan normal SEBELUM mulai merusak apa pun secara sengaja.

1. Cek Kafka topic `opensky.states` terus bertambah (append-only, jalankan 2x dengan jeda beberapa menit — total antar partisi harus naik):
   ```powershell
   & "$env:KAFKA_HOME\bin\windows\kafka-get-offsets.bat" --broker-list localhost:9092 --topic opensky.states --time -1
   ```
2. Cek `live_states` di MongoDB terus bertambah (jalankan 2x dengan jeda beberapa menit):
   ```powershell
   & "<venv>\Scripts\python.exe" -c "from pymongo import MongoClient; c = MongoClient('mongodb://localhost:27017/?replicaSet=rs0&directConnection=true'); print(c.opensky.live_states.count_documents({}))"
   ```
3. Cek ketiga query Structured Streaming `RUNNING` (bukan `FAILED`) — buka `http://localhost:4040/StreamingQuery/`, section "Active Streaming Queries" harus menunjukkan (3), bukan kurang.
4. Buka `http://localhost:5173` tab LIVE — pesawat harus bergerak tanpa refresh manual.
5. Jalankan batch untuk hari ini, harus exit code 0:
   ```powershell
   scripts\run_batch_daily.ps1 -Date (Get-Date).ToString("yyyy-MM-dd")
   ```
6. Buka tab HISTORY — chart per jam + heatmap harus terisi (bukan pesan "belum tersedia").

## Task 1 — Chaos-injection hook di `ingest.py`

Tujuan: menyiapkan cara menyuntik latensi/error ke proses ingest tanpa mengubah kode produksi secara permanen (env var, default mati).

Sudah diimplementasikan di `src/ingest.py` fungsi `fetch_states` — dua env var:
- `CHAOS_LATENCY_S` — sleep (detik) sebelum tiap request
- `CHAOS_ERROR_RATE` — probabilitas (0.0-1.0) melempar error simulasi

Cara pakai manual:
```powershell
$env:CHAOS_ERROR_RATE = "0.2"   # 20% request akan disimulasikan gagal
$env:CHAOS_LATENCY_S = "5"      # tiap request disisipkan sleep 5 detik
python src\ingest.py --once     # atau tanpa --once untuk mode terus-menerus
Remove-Item Env:\CHAOS_ERROR_RATE, Env:\CHAOS_LATENCY_S   # WAJIB dibersihkan setelah selesai
```

## Task 2 — Eksperimen 1: Kill DataNode saat batch job

**Hipotesis:** replikasi HDFS di lab ini cuma 1 (`dfs.replication=1`) — begitu DataNode mati, batch job seharusnya GAGAL total, bukan cuma lambat.

1. Cari PID DataNode dan cek baseline:
   ```powershell
   jps
   & "$env:HADOOP_HOME\bin\hdfs.cmd" dfsadmin -report | Select-String "Live datanodes|Under replicated"
   ```
2. Kill, lalu SEGERA jalankan batch job:
   ```powershell
   taskkill /PID <PID_DATANODE> /F
   scripts\run_batch_daily.ps1 -Date (Get-Date).ToString("yyyy-MM-dd")
   ```
   Perhatikan exit code dan pesan error di output.
3. **Rollback — restart DataNode.** `--daemon` TIDAK didukung oleh `hdfs.cmd` di Windows (`Unrecognized option: --daemon`, fatal exit — itu fitur shell script Linux, tidak ada padanan `.cmd`-nya). Jalankan langsung di window terpisah:
   ```powershell
   Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$Host.UI.RawUI.WindowTitle = 'DataNode'; & `"$env:HADOOP_HOME\bin\hdfs.cmd`" datanode"
   ```
4. Tunggu sampai live lagi, lalu cek kesehatan:
   ```powershell
   & "$env:HADOOP_HOME\bin\hdfs.cmd" dfsadmin -report | Select-String "Live datanodes"
   & "$env:HADOOP_HOME\bin\hdfs.cmd" fsck /bigdata/opensky -files -blocks | Select-String "Status|Missing"
   ```
5. Re-run batch job sekali lagi untuk buktikan sudah pulih total (harus exit code 0).

## Task 3 — Eksperimen 2: Latensi/error di ingest

**Hipotesis:** streaming tetap jalan, latensi naik gracefully, tidak ada data hilang setelah pulih.

1. Catat `live_states` count sebagai baseline (command sama seperti Task 0 langkah 2).
2. Tutup window "Ingest" yang jalan normal, jalankan ulang dengan chaos aktif (lihat cara pakai di Task 1), biarkan ~5 menit.
3. Bersihkan env var, jalankan ingest normal lagi.
4. Buka Spark UI (`http://localhost:4040/StreamingQuery/`) — ketiga query harus tetap `RUNNING` sepanjang periode injeksi (bukan cuma 1-2).
5. Cek `live_states` count lagi — harus naik wajar (bukan turun/stuck).
6. Cek Kafka offset juga naik konsisten selama periode chaos (command sama seperti Task 0 langkah 1) — kegagalan simulasi di `fetch_states` terjadi SEBELUM pesan terkirim ke Kafka, jadi tidak menghasilkan pesan "rusak", cuma lebih sedikit pesan per unit waktu selama chaos aktif.

## Task 4 — Eksperimen 3: Kill proses streaming

**Hipotesis:** recovery dari checkpoint tanpa duplikat, maksimal 1 micro-batch tertunda. Catatan: karena `streaming_job.py` tetap `local[*]` (migrasi YARN dibatalkan), "kill executor" di sini = kill seluruh proses `SparkSubmit` (driver dan executor jadi satu di local mode).

1. Cari PID `SparkSubmit` lewat `jps`, catat batch ID terakhir di Spark UI tab Structured Streaming.
2. Kill: `taskkill /PID <PID_SPARKSUBMIT> /F`
3. Restart — **wajib** set `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` sebelum `spark-submit`, dan **wajib** flag `--packages` (connector Kafka bukan bagian default Spark, tanpa ini restart gagal `ClassNotFoundException`):
   ```powershell
   $env:PYSPARK_PYTHON = "<venv>\Scripts\python.exe"
   $env:PYSPARK_DRIVER_PYTHON = "<venv>\Scripts\python.exe"
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src\streaming_job.py
   ```
4. Cek Spark UI (`http://localhost:4040/StreamingQuery/`) — ketiga query harus balik `RUNNING`, dan Run ID baru tapi **Query ID sama** dengan sebelum kill (bukti resume dari checkpoint, bukan mulai baru dari nol); batch ID harus lanjut dari terakhir.
5. Cek tidak ada duplikat di MongoDB:
   ```powershell
   & "<venv>\Scripts\python.exe" -c "from pymongo import MongoClient; c = MongoClient('mongodb://localhost:27017/?replicaSet=rs0&directConnection=true'); db=c.opensky; print(len(db.live_states.distinct('_id')), db.live_states.count_documents({}))"
   ```
   Kedua angka harus sama (upsert-by-`icao24`, tidak mungkin duplikat by design).

   > **Catatan:** ini memverifikasi `live_states` (aman by design). Koleksi `alerts` (khususnya `density_spike`) **tidak** punya proteksi dedup yang sama — insert polos tanpa key, bisa menghasilkan dokumen duplikat kalau kondisi spike tetap `True` di beberapa trigger berturut-turut sebelum window ditutup. Ini bug pra-eksisting terpisah, di luar scope Eksperimen 3 — lihat `reports/reports-v1.md` §6 "Temuan sampingan".

## Task 5-7 — Demo script, slide outline, finalisasi laporan

Lihat `docs/plans/2026-07-12-m4-validasi-resiliensi.md` Task 5-7 untuk detail lengkap (isinya sudah termasuk kode `scripts/demo.ps1` dan outline slide siap pakai).

## Di mana hasilnya dicatat

- Screenshot & output teks (hdfs ls, mongosh/pymongo count) → `reports/screenshots/` dan `reports/*.txt`
- Hasil tiap eksperimen (hipotesis terbukti/terbantah, MTTR, tindak lanjut) → `docs/design/hasil-eksperimen-resiliensi.md`
- Log naratif per sesi kerja → `reports/` (bebas nama file, mis. `m4-task-log.md`)

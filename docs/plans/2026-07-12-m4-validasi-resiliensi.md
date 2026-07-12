# M4 — Validasi Resiliensi & Persiapan Presentasi — Rencana Implementasi

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Selesaikan milestone terakhir (M4) dari `docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md` — jalankan 3 eksperimen chaos yang sudah dispesifikasikan di §6 desain arsitektur, catat hasilnya, ambil screenshot untuk laporan, lalu finalisasi dokumen desain jadi laporan akhir.

**Architecture:** Tidak ada komponen baru di pipeline. Satu perubahan kecil dan reversibel di `ingest.py` (chaos-injection hook di belakang env var, default mati) untuk Eksperimen 2; Eksperimen 1 & 3 murni operasional (kill proses JVM lokal via `jps`+`taskkill`, lalu amati recovery) — tidak menyentuh kode produksi sama sekali.

**Tech Stack:** PowerShell (orkestrasi eksperimen), `jps`/`taskkill` (JDK bawaan, untuk kill proses target), HDFS CLI (`hdfs.cmd dfsadmin`/`fsck`), Spark UI (`localhost:4040`), `mongosh`.

## Global Constraints

- Blast radius = 0 pengguna eksternal — semua eksperimen dijalankan di lab lokal (mesin ini), tidak ada endpoint publik yang terpengaruh.
- Setiap eksperimen harus reversibel: definisikan langkah rollback SEBELUM menjalankan langkah "kill", dan jangan lanjut ke eksperimen berikutnya sampai rollback eksperimen sebelumnya diverifikasi selesai.
- Jangan gunakan `$ErrorActionPreference = "Stop"` di skrip PowerShell baru yang memanggil CLI Hadoop (`hdfs.cmd`, `jps`, dll) — pelajaran dari sesi 2026-07-11: stderr dari tool Hadoop/Java di-treat sebagai `NativeCommandError` fatal walau exit code sebenarnya 0. Pakai `"Continue"` + cek `$LASTEXITCODE` eksplisit.
- Hasil tiap eksperimen WAJIB dicatat di `docs/design/hasil-eksperimen-resiliensi.md` dengan 3 kolom minimal: hipotesis terbukti/terbantah, waktu pemulihan (MTTR), tindak lanjut — sesuai instruksi asli di master plan baris 249.
- Commit terpisah per task (bukan satu commit besar di akhir) — konsisten dengan preferensi user "solo sequential" (lihat [[feedback-bigdata-workflow]]).
- Sebelum eksperimen dimulai: `run_backend.cmd` harus sudah jalan (HDFS + MongoDB + API + ingest + streaming aktif) — kalau belum, jalankan dulu dan biarkan mengendap beberapa menit supaya ada data live untuk diamati saat proses di-kill.
- Tidak ada task screenshot terpisah — setiap screenshot yang dibutuhkan untuk laporan diambil langsung pada momen yang relevan di Task 0-4, saat layar itu sudah terbuka untuk verifikasi. Simpan semua ke `reports/screenshots/` (dibuat di Task 0 Step 1).

---

### Task 0: Smoke-check M1–M3 masih valid sebelum masuk M4

`what-have-done.md` mencatat M1 (streaming end-to-end), M2 (serving+frontend), M3 (batch) semua sudah diverifikasi di sesi-sesi sebelumnya, tapi checkbox di `docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md` (baris 233-245) belum pernah dicentang. Sebelum eksperimen chaos (yang butuh semua komponen jalan bareng), pastikan status itu masih benar hari ini, dan centang checkbox-nya sebagai catatan resmi.

**Files:**
- Modify: `docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md:233-245` (centang checkbox M1-M3 kalau smoke-check lolos)

- [ ] **Step 1: Pastikan backend jalan, siapkan folder screenshot**

```powershell
scripts\run_backend.cmd
New-Item -ItemType Directory -Force -Path "reports\screenshots" | Out-Null
```

Expected: 3 window baru terbuka (API, Ingest, Streaming Job) tanpa error langsung; window "Streaming Job" menampilkan log batch Structured Streaming setiap ~60 detik.

- [ ] **Step 2: Verifikasi M1 (streaming end-to-end) — sekalian screenshot `spark-ui-streaming.png` & `mongosh.png`**

```powershell
Get-ChildItem "landing-stream-recording\landing_stream" | Sort-Object LastWriteTime -Descending | Select-Object -First 3
```

Expected: file NDJSON terbaru berumur < 90 detik (tier Jawa poll tiap 60 detik).

```powershell
$py = "D:\Coding\#bigdata\venv\Scripts\python.exe"
& $py -c "from pymongo import MongoClient; c = MongoClient('mongodb://localhost:27017/?replicaSet=rs0&directConnection=true'); print('live_states:', c.opensky.live_states.count_documents({}))"
```

Expected: angka > 0, naik dibanding hitungan sesi sebelumnya kalau dijalankan lagi beberapa menit kemudian.

Buka `http://localhost:4040` tab **Structured Streaming** (input rate/batch duration sehat) → screenshot ke `reports/screenshots/spark-ui-streaming.png`. Buka `mongosh`, jalankan `db.live_states.countDocuments()` dan `db.zone_stats.countDocuments()` → screenshot ke `reports/screenshots/mongosh.png`.

- [ ] **Step 3: Verifikasi M2 (serving + frontend) — sekalian screenshot `dashboard-live.png`**

```powershell
cd "d:\Coding\Projects\it-infra-web"; npm run dev
```

Buka `http://localhost:5173`, tab LIVE. Expected: ikon pesawat muncul di atas Indonesia dan bergerak tanpa refresh manual (bandingkan posisi 2 kali dengan jeda 10 detik). Screenshot ke `reports/screenshots/dashboard-live.png`.

- [ ] **Step 4: Verifikasi M3 (batch) — sekalian screenshot `hdfs-ls.png`, `spark-ui-batch-dag.png`, `dashboard-history.png`**

```powershell
$today = (Get-Date).ToString("yyyy-MM-dd")
scripts\run_batch_daily.ps1 -Date $today
```

Expected: exit code 0, tidak ada exception di output.

```powershell
& "$env:HADOOP_HOME\bin\hdfs.cmd" dfs -ls -R /bigdata/opensky/curated
```

Screenshot terminal ini ke `reports/screenshots/hdfs-ls.png`. Buka Spark UI tab **Jobs**, klik job batch terakhir → screenshot DAG-nya ke `reports/screenshots/spark-ui-batch-dag.png`. Lalu buka tab HISTORY di frontend untuk tanggal hari ini — chart per jam dan heatmap kepadatan terisi (bukan pesan "belum tersedia") → screenshot ke `reports/screenshots/dashboard-history.png`.

- [ ] **Step 5: Centang checkbox M1-M3 dan commit**

Edit `docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md` baris 233-245, ganti semua `- [ ]` di bawah M1/M2/M3 jadi `- [x]`.

```bash
git add docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md reports/screenshots/
git commit -m "docs: confirm M1-M3 still hold before starting M4 chaos experiments, add baseline screenshots"
```

---

### Task 1: Tambah chaos-injection hook di `ingest.py` (untuk Eksperimen 2)

**Files:**
- Modify: `src/ingest.py:62-88` (fungsi `fetch_states`)

**Interfaces:**
- Produces: dua env var baru dibaca oleh `fetch_states` — `CHAOS_LATENCY_S` (float detik, default `0`, sleep sebelum request) dan `CHAOS_ERROR_RATE` (float 0.0-1.0, default `0`, probabilitas melempar exception simulasi sebelum request sungguhan dikirim). Default kosong/unset = perilaku persis seperti sebelumnya, jadi tidak ada resiko regresi kalau env var tidak pernah di-set.

- [ ] **Step 1: Tambah import `os` dan `random` di bagian import**

```python
import os
import random
```

(letakkan di antara `import logging` dan `import sys`, urutan alfabetis mengikuti gaya import yang sudah ada di file)

- [ ] **Step 2: Sisipkan hook di awal `fetch_states`, sebelum loop retry**

```python
def fetch_states(cfg, tier, max_retries=3):
    """GET /states/all dengan bbox tier; retry exponential backoff (2/4/8s).

    Chaos hooks (env var, default mati - lihat docs/plans/2026-07-12-m4-validasi-resiliensi.md
    Eksperimen 2): CHAOS_LATENCY_S menyisipkan sleep sebelum tiap request,
    CHAOS_ERROR_RATE melempar error simulasi dengan probabilitas tsb sebelum
    request sungguhan dikirim, supaya retry/backoff asli di bawah ini teruji."""
    chaos_latency = float(os.environ.get("CHAOS_LATENCY_S", "0"))
    chaos_error_rate = float(os.environ.get("CHAOS_ERROR_RATE", "0"))
    bbox = cfg["opensky"]["tiers"][tier]
    params = {
        "lamin": bbox["lamin"],
        "lamax": bbox["lamax"],
        "lomin": bbox["lomin"],
        "lomax": bbox["lomax"],
    }
    last_err = None
    for attempt in range(max_retries):
        try:
            if chaos_latency > 0:
                time.sleep(chaos_latency)
            if chaos_error_rate > 0 and random.random() < chaos_error_rate:
                raise RuntimeError("chaos: simulated ingest error (CHAOS_ERROR_RATE)")
            token = get_token(cfg)
            resp = requests.get(
                cfg["opensky"]["api_url"],
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                timeout=20,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001 - network errors of many shapes
            last_err = exc
            wait = 2 ** (attempt + 1)
            log.warning("fetch_states(%s) attempt %d failed: %s; retry in %ds", tier, attempt + 1, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"fetch_states({tier}) failed after {max_retries} attempts") from last_err
```

- [ ] **Step 3: Verifikasi default (env var unset) tidak berubah perilaku**

```powershell
cd "d:\Coding\#bigdata\it-infra"
$env:PYSPARK_PYTHON = "D:\Coding\#bigdata\venv\Scripts\python.exe"
& "D:\Coding\#bigdata\venv\Scripts\python.exe" src\ingest.py --once
```

Expected: log sama seperti biasa (`tier=java pesawat=N kredit_dipakai=1`, `tier=national ...`), tidak ada baris `chaos: simulated ingest error` karena `CHAOS_ERROR_RATE` default `0`.

- [ ] **Step 4: Verifikasi hook aktif saat env var di-set**

```powershell
$env:CHAOS_ERROR_RATE = "1.0"
& "D:\Coding\#bigdata\venv\Scripts\python.exe" src\ingest.py --once
Remove-Item Env:\CHAOS_ERROR_RATE
```

Expected: 3x baris `WARNING ... chaos: simulated ingest error` (retry attempt 1/2/3) diikuti `RuntimeError: fetch_states(java) failed after 3 attempts` — membuktikan hook benar-benar menembus jalur retry yang sudah ada, bukan cuma dekorasi.

- [ ] **Step 5: Commit**

```bash
git add src/ingest.py
git commit -m "feat: add env-gated chaos hooks (CHAOS_LATENCY_S, CHAOS_ERROR_RATE) to ingest.py fetch_states"
```

---

### Task 2: Eksekusi Eksperimen 1 — Kill DataNode saat batch job

**Hipotesis (dari desain §6):** job `batch_job.py` selesai (exit code 0) bila replikasi ≥2; pada replikasi 1 (setup lab ini, `dfs.replication=1`) job diperkirakan GAGAL — ini didokumentasikan sebagai temuan yang diharapkan, bukan kegagalan tak terduga.

**Files:**
- Create: `docs/design/hasil-eksperimen-resiliensi.md` (dibuat di task ini, diisi lebih lanjut di Task 3 & 4)

- [x] **Step 1: Catat baseline & cari PID DataNode**

```powershell
& "$env:HADOOP_HOME\bin\hdfs.cmd" dfsadmin -report | Select-String "Live datanodes|Under replicated"
jps
```

Expected: `Live datanodes (1)`; `jps` menampilkan baris `<PID> DataNode`. Catat PID-nya dan waktu saat ini (`Get-Date`).

- [x] **Step 2: Kill DataNode, lalu SEGERA jalankan batch job**

```powershell
taskkill /PID <PID_DATANODE> /F
$today = (Get-Date).ToString("yyyy-MM-dd")
scripts\run_batch_daily.ps1 -Date $today
```

Expected (hipotesis): perintah kedua gagal — exit code non-zero, exception seperti `org.apache.hadoop.ipc.RemoteException` / `Could not obtain block` di output `spark-submit`. Simpan output lengkap (copy-paste ke `docs/design/hasil-eksperimen-resiliensi.md`) dan screenshot terminal-nya ke `reports/screenshots/chaos1-batch-fail.png` — ini bukti visual paling penting dari eksperimen ini (kegagalan aktual, bukan cuma catatan).

**Hasil aktual:** terbukti — job abort dengan `SparkException: Job aborted due to stage failure`, root cause `Connection refused` ke port DataNode 9866 + `No live nodes contain block`. Output lengkap ditempel ke `docs/design/hasil-eksperimen-resiliensi.md` (bukan screenshot PNG — teks terminal ditempel langsung, dianggap cukup untuk kasus ini).

- [x] **Step 3: Rollback — restart DataNode**

```powershell
& "$env:HADOOP_HOME\bin\hdfs.cmd" --daemon start datanode
```

**Deviasi ditemukan:** `--daemon` **tidak didukung** oleh `hdfs.cmd` di Windows (`Unrecognized option: --daemon`, fatal exit) — itu fitur `hadoop-functions.sh` (shell Linux) yang tidak ada padanannya di build Windows, dan tidak ada `hadoop-daemon.cmd` di `sbin/` (hanya versi `.sh`). Cara yang benar dipraktikkan: jalankan `hdfs.cmd datanode` langsung (foreground) di window PowerShell terpisah, sama seperti pola window "Ingest"/"Streaming Job" di `run_backend.ps1`. **`docs/design/panduan-manual-m4.md` masih menyebut `--daemon` — perlu diperbarui di sesi berikutnya.**

- [x] **Step 4: Verifikasi pulih & catat MTTR**

```powershell
$maxWait = 60; $waited = 0
do {
    $report = & "$env:HADOOP_HOME\bin\hdfs.cmd" dfsadmin -report 2>&1
    if ($report -match "Live datanodes \(1\)") { break }
    Start-Sleep -Seconds 3; $waited += 3
} while ($waited -lt $maxWait)
Write-Output "DataNode kembali live setelah ~${waited}s"
& "$env:HADOOP_HOME\bin\hdfs.cmd" fsck /bigdata/opensky -files -blocks | Select-String "Status|Missing"
```

Expected: `Status: HEALTHY`, `0 missing blocks`. MTTR = waktu dari Step 2 (kill) sampai DataNode live lagi di Step 4.

**Hasil aktual:** `Live datanodes (1)`, `Status: HEALTHY`, `0 missing blocks`. MTTR teknis murni ~20 detik (dari perintah restart yang benar sampai sehat); total insiden-ke-terverifikasi ~8 menit karena percobaan `--daemon` yang gagal duluan (lihat deviasi Step 3).

- [x] **Step 5: Re-run batch job untuk buktikan pulih total**

```powershell
scripts\run_batch_daily.ps1 -Date $today
```

Expected: exit code 0 (sebelumnya gagal di Step 2, sekarang sukses setelah DataNode pulih).

**Hasil aktual:** exit code 0, 2287 baris bersih, 389 pesawat unik, jam tersibuk 09 — pulih total.

- [x] **Step 6: Tulis hasil ke `docs/design/hasil-eksperimen-resiliensi.md`**

```markdown
# Hasil Eksperimen Resiliensi

## Eksperimen 1 — Kill DataNode saat batch job

- **Hipotesis:** job selesai bila replikasi >=2; pada replikasi 1 job gagal.
- **Hasil:** TERBUKTI — `batch_job.py` gagal dengan [tempel error asli] setelah DataNode di-kill (replikasi 1, sesuai keterbatasan lab yang diakui di §6 desain).
- **MTTR:** [isi detik/menit dari Step 2 ke Step 4]
- **Tindak lanjut:** di produksi, `dfs.replication>=3` menghilangkan single point of failure ini (sudah disebut sebagai keterbatasan lab & jalur produksi di desain §6/§8, sekarang terverifikasi empiris bukan cuma teori).
```

- [ ] **Step 7: Commit**

```bash
git add docs/design/hasil-eksperimen-resiliensi.md
git commit -m "docs: record chaos experiment 1 result (kill DataNode during batch)"
```

---

### Task 3: Eksekusi Eksperimen 2 — Latensi/error di ingest

**Hipotesis (dari desain §6):** streaming lanjut jalan, latensi naik gracefully, tidak ada record hilang setelah pulih.

- [ ] **Step 1: Baseline row count sebelum injeksi**

```powershell
$py = "D:\Coding\#bigdata\venv\Scripts\python.exe"
& $py -c "from pymongo import MongoClient; c = MongoClient('mongodb://localhost:27017/?replicaSet=rs0&directConnection=true'); print('live_states:', c.opensky.live_states.count_documents({}))"
```

Catat angka & waktu.

- [ ] **Step 2: Stop window "Ingest" yang jalan (dari `run_backend`), restart dengan chaos env var, biarkan 5 menit**

Tutup window PowerShell/cmd berjudul "Ingest" (dari `run_backend.cmd`), lalu jalankan foreground supaya log kelihatan langsung:

```powershell
cd "d:\Coding\#bigdata\it-infra"
$env:CHAOS_LATENCY_S = "5"
$env:CHAOS_ERROR_RATE = "0.2"
& "D:\Coding\#bigdata\venv\Scripts\python.exe" src\ingest.py
```

Biarkan jalan ~5 menit (Ctrl+C untuk stop), amati log: sebagian poll menunjukkan `WARNING ... chaos: simulated ingest error` diikuti retry, tapi mayoritas akhirnya sukses (`tier=java pesawat=N ...`) karena peluang gagal 3x berturut-turut hanya 0.2³=0.8%.

- [ ] **Step 3: Rollback env var, restart ingest normal**

```powershell
Remove-Item Env:\CHAOS_LATENCY_S
Remove-Item Env:\CHAOS_ERROR_RATE
```

Jalankan lagi window "Ingest" normal (tanpa env var) — bisa lewat `run_backend.cmd` ulang atau manual `python src\ingest.py` di window baru.

- [ ] **Step 4: Verifikasi Spark UI Structured Streaming tidak error selama periode injeksi**

Buka `http://localhost:4040` (atau port yang tertera di log `streaming_job.py`) tab **Structured Streaming**. Expected: query tetap `RUNNING` sepanjang periode injeksi, `numInputRows` tetap >0 (mungkin sedikit lebih jarang karena beberapa poll gagal & tidak menghasilkan file baru — ini normal, bukan bug, sesuai desain §6 "kalau OpenSky down, tidak ada file baru = tidak ada micro-batch, bukan error").

- [ ] **Step 5: Verifikasi tidak ada record hilang setelah pulih**

```powershell
& $py -c "from pymongo import MongoClient; c = MongoClient('mongodb://localhost:27017/?replicaSet=rs0&directConnection=true'); print('live_states:', c.opensky.live_states.count_documents({}))"
```

Bandingkan dengan Step 1 — expected: angka naik wajar (bukan turun/stuck), konsisten dengan trafik pesawat berjalan normal.

- [ ] **Step 6: Tulis hasil ke `docs/design/hasil-eksperimen-resiliensi.md`** (append di bawah Eksperimen 1)

```markdown
## Eksperimen 2 — Sumber data lambat/error (inject sleep 5s + 20% error rate di ingestor)

- **Hipotesis:** streaming lanjut, latensi naik gracefully, tidak ada record hilang setelah pulih.
- **Hasil:** TERBUKTI — selama ~5 menit injeksi, [X dari Y] poll kena chaos error tapi retry (backoff bawaan `fetch_states`) berhasil memulihkan mayoritas; Spark Structured Streaming query tetap RUNNING, `numInputRows` tidak pernah error meski sempat kosong di batch tanpa file baru. `live_states` count naik dari [angka Step1] ke [angka Step5], tidak ada indikasi data hilang.
- **MTTR:** N/A (tidak ada downtime nyata - sistem tetap berjalan selama injeksi, ini validasi graceful degradation bukan recovery dari kegagalan total)
- **Tindak lanjut:** retry+exponential backoff bawaan `fetch_states` (2/4/8 detik, §desain) terbukti cukup untuk error rate transient sampai 20%; kalau di produksi error rate API jauh lebih tinggi/persisten, perlu circuit breaker + alerting tambahan (di luar scope prototipe ini).
```

- [ ] **Step 7: Commit**

```bash
git add docs/design/hasil-eksperimen-resiliensi.md
git commit -m "docs: record chaos experiment 2 result (ingest latency/error injection)"
```

---

### Task 4: Eksekusi Eksperimen 3 — Kill executor saat streaming

**Hipotesis (dari desain §6):** maksimal 1 micro-batch tertunda; recovery dari checkpoint tanpa duplikat. Catatan penting yang sudah diakui di desain: karena `streaming_job.py` tetap `local[*]` (migrasi YARN dibatalkan), "kill executor" di sini berarti kill proses `SparkSubmit` itu sendiri (driver dan executor adalah proses yang sama di local mode) — hipotesis diuji lewat restart query dari checkpoint, bukan lewat kill 1 dari N executor terpisah.

- [ ] **Step 1: Cari PID proses streaming & catat progress checkpoint**

```powershell
jps
```

Expected: baris `<PID> SparkSubmit` (window "Streaming Job"). Di Spark UI (`http://localhost:4040`, tab Structured Streaming), catat batch ID terakhir yang sukses diproses dan `processedRowsPerSecond`.

- [ ] **Step 2: Kill proses, catat waktu**

```powershell
taskkill /PID <PID_SPARKSUBMIT> /F
Get-Date
```

- [ ] **Step 3: Restart streaming_job.py dari checkpoint**

```powershell
cd "d:\Coding\#bigdata\it-infra"
$env:PYSPARK_PYTHON = "D:\Coding\#bigdata\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "D:\Coding\#bigdata\venv\Scripts\python.exe"
spark-submit src\streaming_job.py
```

- [ ] **Step 4: Verifikasi recovery & catat MTTR**

Amati Spark UI setelah restart: expected batch pertama setelah restart melanjutkan dari checkpoint (batch ID lanjut dari terakhir, bukan mulai dari 0), `processedRowsPerSecond` kembali stabil dalam beberapa detik. MTTR = waktu dari Step 2 (kill) sampai batch pertama sukses setelah restart di Step 4. Screenshot tab Structured Streaming yang menunjukkan kontinuitas batch ID ini ke `reports/screenshots/chaos3-streaming-recovery.png` — bukti visual utama recovery-from-checkpoint.

- [ ] **Step 5: Verifikasi tidak ada duplikat di MongoDB**

```powershell
& $py -c "from pymongo import MongoClient; c = MongoClient('mongodb://localhost:27017/?replicaSet=rs0&directConnection=true'); print('live_states distinct icao24:', len(c.opensky.live_states.distinct('_id')), '| total docs:', c.opensky.live_states.count_documents({}))"
```

Expected: kedua angka sama (koleksi `live_states` di-upsert berdasarkan `icao24` sebagai `_id`, jadi tidak mungkin duplikat berdasarkan desain sink-nya — konfirmasi ini, bukan cuma asumsi).

- [ ] **Step 6: Tulis hasil ke `docs/design/hasil-eksperimen-resiliensi.md`** (append di bawah Eksperimen 2)

```markdown
## Eksperimen 3 — Kill proses streaming (taskkill SparkSubmit, `local[*]`)

- **Hipotesis:** maksimal 1 micro-batch tertunda; recovery dari checkpoint tanpa duplikat.
- **Hasil:** TERBUKTI — setelah `taskkill`, restart `spark-submit src\streaming_job.py` melanjutkan dari checkpoint batch ID [N] (bukan dari 0), `live_states` tidak menunjukkan duplikat (distinct count == total count berkat upsert-by-`icao24`).
- **MTTR:** [isi detik dari Step 2 ke Step 4]
- **Tindak lanjut:** karena tetap `local[*]` (migrasi YARN dibatalkan permanen, lihat `2026-07-11-yarn-migration-design.md`), "kill executor" di sini setara kill seluruh query - di produksi dengan resource manager sungguhan (YARN/K8s), hanya 1 executor yang perlu di-relaunch tanpa mematikan driver, MTTR seharusnya lebih rendah dari yang terukur di sini.
```

- [ ] **Step 7: Commit**

```bash
git add docs/design/hasil-eksperimen-resiliensi.md
git commit -m "docs: record chaos experiment 3 result (kill streaming process, recover from checkpoint)"
```

---

### Task 5: `scripts/demo.ps1` — skenario demo ±15 menit

**Files:**
- Create: `scripts/demo.ps1`

**Interfaces:**
- Consumes: `scripts/run_backend.ps1` (Task ini memanggilnya, tidak menduplikasi logikanya)

- [ ] **Step 1: Tulis skrip mengikuti skenario §7 desain (baris 209-214 dokumen arsitektur)**

```powershell
# Skenario demo end-to-end (+-15 menit) - lihat docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md #7
Write-Output "=== DEMO: 1/4 Start infra (HDFS + MongoDB) ==="
& (Join-Path $PSScriptRoot "start_infra.ps1")
Write-Output "Tunjukkan: file JSON masuk ke HDFS raw/ dan landing_stream/ tiap ~60 detik (jalankan ingest.py di window terpisah setelah ini)."
Read-Host "Tekan Enter setelah menjalankan run_backend.cmd di window lain dan menunggu ~1 menit data pertama masuk"

Write-Output "=== DEMO: 2/4 Structured Streaming ==="
Write-Output "Buka http://localhost:4040 tab Structured Streaming - tunjukkan input rate & batch duration."
Write-Output "Buka mongosh: db.live_states.countDocuments() - tunjukkan terisi."
Read-Host "Tekan Enter untuk lanjut ke batch"

Write-Output "=== DEMO: 3/4 Batch job ==="
$today = (Get-Date).ToString("yyyy-MM-dd")
& (Join-Path $PSScriptRoot "run_batch_daily.ps1") -Date $today
Write-Output "Tunjukkan: hdfs dfs -ls /bigdata/opensky/curated dan DAG job di Spark UI tab Jobs."
Read-Host "Tekan Enter untuk lanjut ke dashboard"

Write-Output "=== DEMO: 4/4 Dashboard ==="
Write-Output "Jalankan 'npm run dev' di repo it-infra-web, buka http://localhost:5173"
Write-Output "Tunjukkan: peta LIVE bergerak tanpa refresh, tab HISTORY terisi, alert panel (trigger lewat replay.py rekaman bersquawk darurat kalau perlu)."
```

- [ ] **Step 2: Jalankan sekali penuh sebagai latihan, catat total waktu**

```powershell
scripts\demo.ps1
```

Expected: total waktu jalan (termasuk jeda manual buka browser/mongosh) mendekati 15 menit; kalau jauh melebihi, pangkas jeda `Read-Host` di skrip.

- [ ] **Step 3: Commit**

```bash
git add scripts/demo.ps1
git commit -m "feat: add scripts/demo.ps1 for the +-15 minute end-to-end demo scenario"
```

---

### Task 6: Outline slide presentasi

**Files:**
- Create: `docs/design/slide-outline.md`

- [ ] **Step 1: Tulis outline mengikuti alur wajib di master plan baris 252** (masalah → arsitektur → justifikasi platform → demo live → hasil chaos → keterbatasan & jalur produksi)

```markdown
# Outline Slide Presentasi — Monitoring Lalu Lintas Udara Indonesia

1. **Masalah**: kebutuhan monitoring lalu lintas udara real-time + historis untuk wilayah Indonesia, sumber data OpenSky Network (gratis, kuota 4000 kredit/hari)
2. **Arsitektur**: diagram Lambda §3 dokumen desain (ingest 2-tier → HDFS raw + landing → Spark streaming+batch → MongoDB+Parquet → FastAPI → React)
3. **Justifikasi platform**: tabel §5 (kenapa Spark bukan Flink, kenapa local[*] bukan YARN — termasuk temuan bug Hadoop-on-Windows, kenapa FastAPI, kenapa deck.gl/MapLibre)
4. **Demo live**: jalankan `scripts/demo.ps1` di depan audiens (~15 menit) — screenshot cadangan di `reports/screenshots/` kalau demo live gagal
5. **Hasil chaos**: ringkasan 3 eksperimen dari `docs/design/hasil-eksperimen-resiliensi.md` (hipotesis terbukti/terbantah + MTTR masing-masing)
6. **Keterbatasan & jalur produksi**: §8 dokumen desain (dfs.replication=1, file-source bukan Kafka, local[*] bukan cluster resource manager sungguhan — dengan bukti empiris dari eksperimen chaos, bukan cuma klaim teoretis)
```

- [ ] **Step 2: Commit**

```bash
git add docs/design/slide-outline.md
git commit -m "docs: add presentation slide outline for M4 wrap-up"
```

---

### Task 7: Finalisasi dokumen desain jadi laporan akhir

**Files:**
- Modify: `docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md` (§6, tambahkan ringkasan hasil eksperimen + link ke `hasil-eksperimen-resiliensi.md` dan screenshot)
- Modify: `docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md:249-253` (centang checkbox M4)

- [ ] **Step 1: Tambah subsection "Hasil Validasi" di §6 dokumen desain, setelah tabel eksperimen (baris 186)**

```markdown
### Hasil Validasi (dieksekusi 2026-07-12+, lihat `docs/design/hasil-eksperimen-resiliensi.md` untuk detail penuh + MTTR)

Ketiga eksperimen chaos dijalankan sungguhan (bukan tabletop) di mesin lab ini. Ringkasan:

1. Kill DataNode saat batch — hipotesis TERBUKTI (job gagal pada replikasi 1, sesuai keterbatasan yang diakui di §6/§8)
2. Latensi/error di ingest — hipotesis TERBUKTI (streaming tetap RUNNING, tidak ada data hilang, retry bawaan cukup untuk error rate 20%)
3. Kill proses streaming — hipotesis TERBUKTI (recovery dari checkpoint, tidak ada duplikat berkat upsert-by-`icao24`)

Screenshot pendukung: `reports/screenshots/`.
```

- [ ] **Step 2: Centang checkbox M4 di master plan**

Ganti semua `- [ ]` di baris 249-253 jadi `- [x]`.

- [ ] **Step 3: Commit final**

```bash
git add docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md
git commit -m "docs: finalize architecture doc as final report, mark M4 complete"
```

---

## Ringkasan Urutan Eksekusi

Task 0 (smoke-check, sekalian screenshot baseline) → Task 1 (chaos hook di ingest.py) → Task 2, 3, 4 (3 eksperimen, berurutan — masing-masing harus rollback+verified sebelum lanjut ke berikutnya, jangan paralel karena semua berbagi infra yang sama; screenshot bukti chaos diambil inline di Task 2 & 4) → Task 5 (demo script) → Task 6 (outline slide) → Task 7 (finalisasi laporan).

Estimasi realistis: 1 sesi kerja penuh untuk Task 0-4 (butuh perhatian penuh saat kill/restart proses), 1 sesi lebih pendek untuk Task 5-7.

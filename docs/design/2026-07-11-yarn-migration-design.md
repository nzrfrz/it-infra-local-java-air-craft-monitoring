# Design: Jalankan Spark job lewat YARN (bukan `local[*]`)

## Problem

Semua `spark-submit` di project ini (`streaming_job.py`, `batch_job.py`) tidak
pernah men-set `--master`, jadi Spark selalu jatuh ke default: **`local[*]`**
(satu JVM, semua core lokal, tanpa resource manager). Ini keputusan desain
yang eksplisit didokumentasikan sebagai batasan skala di
`2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md` §7 (baris
"Cluster: single-node, replikasi 1" vs skala produksi "multi-node
YARN/Kubernetes").

Hadoop yang sudah ter-install di mesin ini **sudah termasuk YARN**, dan
ResourceManager + NodeManager **sudah bisa dinyalakan dan sudah pernah
dites jalan** (dikonfirmasi: `jps` menunjukkan `ResourceManager` +
`NodeManager` aktif, RM REST API `http://localhost:8088/ws/v1/cluster/metrics`
melaporkan 1 NodeManager aktif dengan kapasitas 8192 MB / 8 vcores). Belum
ada aplikasi yang pernah disubmit ke situ — Spark selalu jalan local.

Goal: submit `streaming_job.py` dan `batch_job.py` ke YARN
(`--master yarn --deploy-mode client`) sebagai resource manager, alih-alih
`local[*]`, supaya arsitektur project ini benar-benar memakai pemisahan
resource-manager/executor yang didokumentasikan, bukan cuma di-skip lewat
default Spark.

## Yang TIDAK berubah

- **HDFS**: `ingest.py` (WebHDFS REST) dan `api/hdfs_read.py` (WebHDFS REST)
  sama sekali tidak lewat Spark, jadi tidak terpengaruh perubahan ini.
- **MongoDB**: `streaming_job.py`/`batch_job.py` sudah nulis lewat `pymongo`
  langsung di `foreachBatch`/di driver (bukan mongo-spark-connector) — jalur
  ini juga tidak terpengaruh, koneksi pymongo jalan dari proses driver yang
  sama seperti sekarang.
- **Kontrak API/data** (`CONTRACTS.md` C1–C4): tidak ada perubahan skema atau
  endpoint. ini murni perubahan *bagaimana* Spark job dieksekusi, bukan *apa*
  yang dihasilkan.
- **`api/main.py`'s `/api/history/refresh`**: tetap `asyncio.create_subprocess_exec("spark-submit.cmd", "src/batch_job.py", "--date", date, ...)`,
  cuma argumennya nanti nambah `--master yarn` (lihat §Perubahan konfigurasi).

## Kenapa `client` mode, bukan `cluster` mode

Dua pilihan deploy-mode YARN: `client` (driver jalan di mesin yang
men-submit, cuma executor yang jadi container YARN) vs `cluster` (driver
*juga* jadi container YARN, submit lalu lepas tangan).

Pilih **`client`**, karena:
- `run_backend.ps1` sengaja membuka window terpisah per komponen supaya
  lognya bisa dipantau live (`Start-Component` di script itu) — itu cuma
  masuk akal kalau driver Spark tetap jalan sebagai proses lokal yang
  attached ke window itu. `cluster` mode bikin window itu cuma nunjukkan
  "submitted, tracking URL: ..." lalu exit, log sesungguhnya pindah ke YARN.
- `streaming_job.py` itu proses **long-running** (Structured Streaming,
  jalan terus). Di `client` mode, mematikan proses driver (Ctrl+C di window
  itu, atau `taskkill`) langsung menghentikan job dengan bersih — pola yang
  sama seperti sekarang. Di `cluster` mode, menghentikan job butuh
  `yarn application -kill <appId>` terpisah, bukan sekadar tutup window.
- Single-node deployment (driver dan satu-satunya NodeManager di mesin yang
  sama) — tidak ada keuntungan lokalitas dari `cluster` mode di sini.

## Perubahan konfigurasi

### 1. `HADOOP_CONF_DIR` / `YARN_CONF_DIR`

Spark butuh ini untuk tahu ke mana connect sebagai ResourceManager. Belum
pernah di-set di mana pun di project ini (`echo $HADOOP_CONF_DIR` kosong).
Tambahkan di **setiap tempat `spark-submit` dipanggil** untuk
`streaming_job.py`/`batch_job.py`, dengan pola yang sama seperti
`PYSPARK_PYTHON` sudah di-set sekarang (env var *sebelum* `spark-submit`
dipanggil, bukan dari dalam `.py` — pelajaran dari bug PYSPARK_PYTHON
sebelumnya, lihat `what-have-done.md` §Sesi 2026-07-10):

```
HADOOP_CONF_DIR=C:\Hadoop\hadoop-3.3.6\etc\hadoop
YARN_CONF_DIR=C:\Hadoop\hadoop-3.3.6\etc\hadoop
```

Tempat yang perlu diupdate:
- `scripts/run_backend.ps1` — `Start-Component` untuk "Streaming Job" dan
  langkah batch_job.py hari-ini (5/5)
- `scripts/run_batch_daily.ps1`
- `api/main.py`'s `history_refresh()` — subprocess env (`asyncio.create_subprocess_exec` menerima parameter `env=`; saat ini tidak di-pass sama
  sekali jadi otomatis inherit environment proses API — cukup pastikan
  proses API sendiri (launcher `.cmd`) sudah punya var ini sebelum start)

### 2. Argumen `spark-submit`

```
spark-submit --master yarn --deploy-mode client src\streaming_job.py
spark-submit --master yarn --deploy-mode client src\batch_job.py --date <tanggal>
```

### 3. Sizing executor

Data harian di skala project ini kecil (ratusan-ribuan baris/hari, sudah
didokumentasikan di §Track B/C `what-have-done.md`). Mesin ini **juga**
menjalankan NameNode+DataNode+ResourceManager+NodeManager+MongoDB+FastAPI+
`ingest.py` di RAM yang sama (total 16 GB fisik) di luar YARN sama sekali —
kapasitas default NodeManager (8192 MB, dicek lewat RM REST API) menyisakan
cuma separuh RAM mesin untuk semua proses non-YARN itu. **Diputuskan:
turunkan `yarn.nodemanager.resource.memory-mb` ke 4096** (biarkan
`cpu-vcores` di default 8, sesuai jumlah core fisik mesin ini — bukan RAM
jadi tidak ada urgensi diturunkan) lewat `yarn-site.xml`:

```xml
<property>
  <name>yarn.nodemanager.resource.memory-mb</name>
  <value>4096</value>
</property>
```
(butuh restart NodeManager setelah diubah)

**Penting soal `client` mode**: di `--deploy-mode client`, yang jadi
container YARN (dialokasikan dari 4096 MB NodeManager) itu cuma
**ApplicationMaster + executor** — `spark.driver.memory` jalan sebagai
proses lokal biasa di mesin yang men-submit, **tidak** dialokasikan YARN
sama sekali (beda dari `cluster` mode, yang sengaja tidak dipakai di sini —
lihat §Kenapa `client` mode). Jadi `spark.driver.memory` tidak masuk hitungan
budget 4096 MB ini.

Yang masuk hitungan budget 4096 MB per **satu aplikasi Spark** (satu
`spark-submit` = satu YARN application, AM + executor-nya):
```
--conf spark.yarn.am.memory=512m       (default Spark, tidak perlu diset eksplisit)
--conf spark.executor.instances=1
--conf spark.executor.memory=512m
--conf spark.executor.cores=1
```
Overhead YARN per container (AM maupun executor) minimal ±384 MB (lantai
`max(384m, 10% dari memory container)` bawaan Spark) — jadi 1 AM container
≈ 512+384 = **896 MB**, 1 executor container ≈ 512+384 = **896 MB**, total
**≈1792 MB per aplikasi**.

`streaming_job.py` (jalan terus) dan `batch_job.py` (jalan sebentar saat
tombol Update dipicu) adalah **dua aplikasi YARN terpisah** yang perlu
alokasi sendiri-sendiri kalau kebetulan jalan bersamaan — bukan berbagi satu
AM/executor. Total kalau keduanya jalan bersamaan: **2 × 1792 MB ≈ 3584 MB**,
masih di bawah 4096 MB (sisa ±512 MB headroom). Ini alasan kenapa
**`spark.executor.instances` sengaja diset ke 1, bukan 2** — dengan 2
executor per aplikasi, dua aplikasi berjalan bersamaan akan minta ≈4480 MB,
melebihi budget NodeManager. Data harian di skala project ini kecil
(ratusan-ribuan baris/hari), jadi 1 executor per aplikasi cukup — tidak ada
alasan menaikkannya kecuali terbukti perlu lewat pengujian nyata.

Kalau di praktiknya masih kena `OutOfMemoryError`/container di-kill YARN
karena melebihi batas, turunkan `spark.executor.memory` dulu (mis. ke 384m)
sebelum menaikkan `yarn.nodemanager.resource.memory-mb` kembali — jangan
langsung naikkan alokasi NodeManager, itu mengorbankan RAM yang disisakan
untuk HDFS/Mongo/API di mesin yang sama.

### 4. Python di YARN container

`os.environ.setdefault("PYSPARK_PYTHON", sys.executable)` yang sudah ada di
`streaming_job.py`/`batch_job.py` **hanya berlaku untuk proses driver**
lokal — itu sudah terbukti dari bug yang sama persis waktu isu ini pertama
ditemukan untuk *driver* Spark (`what-have-done.md` §Sesi 2026-07-10, poin
soal `PYSPARK_PYTHON` harus di-set sebelum `spark-submit` dipanggil, bukan
dari dalam `.py`, karena driver process keburu start). Di YARN, *executor*
jalan sebagai container terpisah yang tidak mewarisi environment proses
driver — butuh eksplisit:
```
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=<path venv python.exe>
--conf spark.executorEnv.PYSPARK_PYTHON=<path venv python.exe>
```
(sama seperti `PYSPARK_PYTHON` yang sudah dipakai; path venv sama karena
single-node, NodeManager jalan di mesin yang sama dengan venv bersama.)

`spark.sparkContext.addPyFile(common.py)` yang sudah ada di kedua job
seharusnya tetap jalan tanpa perubahan — itu API Spark yang didesain untuk
mendistribusikan file ke executor lewat mekanisme dist-cache-nya YARN
sendiri, jadi sudah "YARN-aware" dari sono-nya. Perlu diverifikasi saat
implementasi (lihat §Verifikasi), bukan diasumsikan aman.

### Ringkasan: flag lengkap gabungan §2–§4

```
spark-submit --master yarn --deploy-mode client ^
  --conf spark.executor.instances=1 ^
  --conf spark.executor.memory=512m ^
  --conf spark.executor.cores=1 ^
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=<path venv python.exe> ^
  --conf spark.executorEnv.PYSPARK_PYTHON=<path venv python.exe> ^
  src\batch_job.py --date <tanggal>
```
(sama untuk `src\streaming_job.py`, tanpa `--date`)

## Risiko (spesifik Windows, mesin ini)

Project ini sudah berkali-kali ketemu bug spesifik Windows+Hadoop
(`hdfs.cmd` motong argumen `=`, `PYSPARK_PYTHON` ke-intersep stub Microsoft
Store, IPv6 link-local bikin WebHDFS lambat, `NotImplementedError` gara-gara
event loop, static partition overwrite Spark, dst — semua di
`what-have-done.md`). YARN container launch di Windows historically juga
rawan masalah spesifik platform yang belum pernah diuji di project ini:

1. **NodeManager container launch di Windows** butuh `winutils.exe` dan
   `hadoop.dll` konsisten di PATH untuk proses yang men-spawn container
   (bukan cuma untuk HDFS CLI yang sudah terbukti jalan) — belum pernah
   diverifikasi untuk jalur YARN container launch spesifik.
2. **Log YARN pindah tempat**: begitu executor jalan sebagai container,
   log-nya *tidak lagi* muncul di window `spark-submit` (cuma driver log
   yang muncul di situ). Debugging jadi butuh `yarn logs -applicationId
   <id>` atau UI ResourceManager (`http://localhost:8088`) — workflow baru
   yang perlu dibiasakan, beda dari kebiasaan "lihat semua log di window
   yang sama" yang dipakai project ini sekarang.
3. **Resource contention**: menambah kapasitas YARN reservation (4096 MB
   setelah diturunkan, lihat §Sizing executor) di atas mesin yang sudah
   menjalankan HDFS+Mongo+API+ingest+IDE+browser bisa
   bikin sistem berat kalau tidak dituning (lihat §Sizing executor).
4. **`streaming_job.py` yang long-running** memegang executor container
   terus-menerus selama job jalan. Kalau `batch_job.py` dipicu lewat tombol
   Update History (`POST /api/history/refresh`) *selagi* `streaming_job.py`
   masih memegang container, kapasitas NodeManager harus cukup untuk
   keduanya sekaligus (sudah dihitung di §Sizing executor, tapi perlu
   diverifikasi nyata, bukan cuma dihitung di atas kertas).

**Sikap terhadap risiko ini**: bukan alasan untuk tidak coba, tapi alasan
untuk **implementasi bertahap dengan checkpoint verifikasi di tiap
langkah** (lihat §Rencana implementasi/rollout), bukan sekali ganti semua
lalu debug kalau rusak.

## Sinergi dengan M1–M4 (chaos experiments)

`what-have-done.md` mencatat M1-M4 (belum dikerjakan) butuh 3 eksperimen
chaos, salah satunya "kill streaming executor". Begitu `streaming_job.py`
jalan di YARN, "kill executor" jadi eksperimen yang **jauh lebih realistis
dan terukur** — bisa benar-benar `yarn application -kill` / matikan
NodeManager dan amati YARN merestart container / re-schedule, dibanding
sekadar `taskkill` proses JVM lokal seperti yang mungkin terpikir sebelum
ini. Bukan alasan untuk digabung sekarang (scope dokumen ini cuma migrasi
eksekusi ke YARN), tapi dicatat sebagai konteks kenapa investasi ini bukan
cuma "supaya sesuai dokumen desain" — juga langsung berguna untuk milestone
yang belum dikerjakan.

## Rencana implementasi / rollout (bertahap, tiap langkah diverifikasi)

1. Turunkan `yarn.nodemanager.resource.memory-mb` ke 4096 di `yarn-site.xml`
   (lihat §Sizing executor), restart NodeManager, cek lewat
   `http://localhost:8088/ws/v1/cluster/metrics` bahwa `totalMB` sekarang
   4096 sebelum lanjut ke langkah berikutnya.
2. Set `HADOOP_CONF_DIR`/`YARN_CONF_DIR` + jalankan `batch_job.py` (job
   pendek, sekali jalan, gampang diulang kalau gagal) manual dari terminal
   dengan `--master yarn` untuk satu tanggal yang sudah diketahui hasilnya
   dari run local mode sebelumnya (mis. `2026-07-10`, sudah tercatat
   `total_records=4404 unique_aircraft=593 busiest_hour=18` di
   `what-have-done.md`). Bandingkan angkanya persis sama.
3. Cek `http://localhost:8088` — pastikan aplikasi muncul di RM UI, cek
   status SUCCEEDED, cek log executor lewat UI itu (bukan window terminal).
4. Baru kalau langkah 2-3 sukses bersih, coba `streaming_job.py` dengan
   `--master yarn` manual (bukan lewat `run_backend.ps1` dulu) — verifikasi
   `live_states`/`zone_stats`/`alerts` di Mongo terisi seperti biasa, cek
   checkpoint HDFS tetap jalan normal.
5. Baru kalau langkah 4 stabil beberapa menit, pindahkan ke
   `run_backend.ps1`/`run_batch_daily.ps1`/`api/main.py` permanen.
6. Update `README.md` dan `what-have-done.md` dengan langkah baru + apa
   yang berubah dari sisi cara debug (lihat §Risiko poin 2).

Di tiap langkah, kalau gagal: **local `local[*]` tetap jadi fallback yang
valid** (cukup hapus `--master yarn` dan flag terkait) — bukan pilihan
biner "YARN semua atau tidak sama sekali", tapi tiap komponen
(`batch_job.py` vs `streaming_job.py`) bisa independen pakai YARN atau
local sampai keduanya benar-benar stabil.

## Out of scope

- Multi-node YARN (tetap 1 NodeManager, di mesin yang sama) — ini menutup
  sebagian kesenjangan dokumentasi §"Cluster: single-node vs multi-node",
  tapi tidak sepenuhnya (masih single-node, cuma resource-manager-nya
  eksplisit dipakai).
- `deploy-mode cluster`.
- YARN High Availability (HA ResourceManager), Capacity/Fair Scheduler
  queue tuning di luar default, Kerberos/keamanan (konsisten dengan
  batasan keamanan yang sudah didokumentasikan di seluruh project ini).
- Dynamic resource allocation / autoscaling executor.
- Migrasi `ingest.py`/`api/main.py` ke YARN — keduanya bukan aplikasi
  Spark, tidak relevan.

# Outline Slide Presentasi — Monitoring Lalu Lintas Udara Indonesia

Alur wajib mengikuti master plan (`docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md` baris 252): masalah → arsitektur → justifikasi platform → demo live → hasil chaos → keterbatasan & jalur produksi.

1. **Masalah**
   - Kebutuhan monitoring lalu lintas udara real-time + historis untuk wilayah Indonesia.
   - Sumber data: OpenSky Network REST API (gratis, live-only, kuota 4000 kredit/hari) — tidak ada endpoint historis, jadi sistem sendiri yang harus menyimpan histori.

2. **Arsitektur**
   - Diagram Lambda §3 dokumen desain (`2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md`): ingest 2-tier (Java + nasional) → HDFS `raw/` (arsip) + `landing_stream/` (NDJSON) → Spark Structured Streaming (speed layer) + Spark batch harian (batch layer) → MongoDB (`live_states`/`zone_stats`/`alerts`) + Parquet curated → FastAPI (REST + WebSocket `/ws/live`) → React frontend (deck.gl + MapLibre).

3. **Justifikasi platform** (tabel §5 dokumen desain)
   - Spark dipilih atas Flink: satu engine untuk streaming+batch, ekosistem lebih matang untuk kebutuhan mahasiswa/lab.
   - `local[*]` dipilih atas YARN sebagai resource manager Spark **setelah dicoba dan gagal secara empiris** — bug classpath jar Hadoop-3.3.6-on-Windows (`{{PWD}}`/`<CPS>` placeholder tidak ter-expand, `ExecutorLauncher`/`ApplicationMaster` class hilang dari classpath container), dibuktikan lewat 5 percobaan reproduksi berbeda. YARN tetap dipakai lewat jalur MapReduce native (Hadoop Streaming) untuk job pembanding `density_grid_hourly` — YARN tidak menganggur, hanya Spark-on-YARN yang tidak viable di platform ini.
   - FastAPI dipilih untuk serving layer: async native, WebSocket built-in, cocok untuk `/ws/live`.
   - deck.gl + MapLibre untuk frontend: rendering ribuan ikon pesawat + heatmap kepadatan performan di browser tanpa backend rendering tambahan.

4. **Demo live**
   - Jalankan `scripts\demo.ps1` di depan audiens (~15 menit): infra check → streaming (Spark UI + `live_states` bertambah) → batch job (HDFS curated + Spark UI DAG) → dashboard (`localhost:5173` tab LIVE bergerak real-time, tab HISTORY terisi).
   - Screenshot cadangan tersedia di `reports/screenshots/` kalau demo live gagal saat presentasi.

5. **Hasil validasi resiliensi (chaos experiments)**
   - Ringkasan dari `docs/design/hasil-eksperimen-resiliensi.md` (3 eksperimen, semua hipotesis TERBUKTI):
     1. **Kill DataNode saat batch** — batch gagal total pada replikasi 1 (sesuai keterbatasan lab yang diakui), MTTR pemulihan teknis ~20 detik, pulih total setelah restart (exit code 0, 2287 baris bersih).
     2. **Latensi/error di ingest** (5s sleep + 20% error rate, ~6 menit) — streaming tetap `RUNNING`, tidak ada data hilang (`live_states` 1358→1418), retry+backoff bawaan (`fetch_states`, 2/4/8 detik) cukup menyerap error rate transient sampai 20%.
     3. **Kill proses streaming** — recovery dari checkpoint (batch ID lanjut, bukan reset ke 0), tanpa duplikat (`live_states` distinct == total, upsert-by-`icao24`), MTTR teknis ~3m39s.
   - Temuan sampingan yang jujur dilaporkan (bukan ditutupi): satu query Structured Streaming (`zone_stats`/`alerts`, dugaan) sempat `FAILED` secara independen dari chaos experiment; `ingest.py` sempat hang diam-diam ~26 menit tanpa exception sebelum pulih sendiri — keduanya dicatat sebagai item investigasi lanjutan, bukan disembunyikan dari laporan.

6. **Keterbatasan & jalur produksi** (§8 dokumen desain, sekarang dengan bukti empiris bukan cuma klaim teoretis)
   - `dfs.replication=1` → single point of failure terverifikasi lewat Eksperimen 1; produksi butuh `>=3`.
   - Sumber data file-based (`landing_stream/`), bukan message queue (Kafka) → cukup untuk skala lab, tapi tidak tahan terhadap backlog besar di produksi.
   - `local[*]`, bukan resource manager cluster sungguhan → Eksperimen 3 menunjukkan kill-executor di local mode = kill seluruh query; di produksi (YARN/K8s) cukup relaunch 1 executor tanpa mematikan driver, MTTR seharusnya lebih rendah.
   - OpenSky free tier live-only, kuota 4000 kredit/hari → membatasi granularitas tier & frekuensi polling; tidak bisa backfill data historis yang belum pernah di-ingest.

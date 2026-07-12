# M4 — Log Eksekusi

Catatan hasil eksekusi tiap task M4 (angka, temuan, commit) — untuk cara/prosedurnya lihat `docs/design/panduan-manual-m4.md`.

## 2026-07-12 — Task 0: Smoke-check M1-M3

- M1 (streaming): file landing terbaru berumur ~47 detik, `live_states` 1331+ dokumen dan terus bertambah, Spark UI Structured Streaming 2 query `RUNNING` sehat.
- M2 (serving+frontend): dashboard LIVE menampilkan pesawat bergerak (`reports/screenshots/dashboard-live.png`).
- M3 (batch): `batch_job.py` hari ini exit code 0 — 1111 baris bersih dari 1517 baris mentah, 243 pesawat unik. Tab HISTORY terisi (`reports/screenshots/dashboard-history.png`).
- Data curated 4 tanggal (07-09 s/d 07-12) semua utuh — `reports/hdfs-ls-curated.txt`.
- Belum diuji ulang: alert toast (squawk 7700 via `replay.py`) — checkbox dibiarkan kosong di master plan.
- Temuan sampingan: 1 query `FAILED` lama (`py4j.Py4JException`) di tab Completed Streaming Queries Spark UI — kemungkinan sisa sesi sebelumnya, bukan dari Task 0 ini. 2 query aktif tetap sehat.
- Sekalian diperbaiki: bug visual radar-sweep di `it-infra-web` (pusat rotasi tidak presisi karena kotak `conic-gradient` tidak persegi + konflik CSS `translate`/`transform` di Tailwind v4).
- Commit: `it-infra` `b57df63`, `it-infra-web` `11bf464` (disinkron ke subtree `web/`).

## 2026-07-12 — Task 1: Chaos-injection hook di `ingest.py`

- Ditambahkan `CHAOS_LATENCY_S` dan `CHAOS_ERROR_RATE` (env var, default mati) di `fetch_states`.
- Verifikasi default: `python src\ingest.py --once` tanpa env var berjalan normal (`tier=java pesawat=34 kredit_dipakai=2`, dst), tidak ada baris chaos.
- Verifikasi aktif: `CHAOS_ERROR_RATE=1.0` membuat tiap tier gagal 3x berturut-turut dengan retry backoff asli 2s/4s/8s, lalu `poll_once(...) gagal` — hook menembus jalur retry yang sudah ada.
- Commit: `it-infra` `561d3f6`.

## 2026-07-12 — Task 2: Eksperimen 1 (kill DataNode) — selesai, dipraktikkan manual oleh user

- Baseline: `Live datanodes (1)`, `Under replicated blocks: 0`, PID DataNode = 18072.
- Kill (`taskkill /PID 18072 /F`) + langsung jalankan batch -> **gagal total**, `SparkException: Job aborted due to stage failure` (Connection refused ke DataNode, block tidak terbaca).
- Rollback: `hdfs.cmd --daemon start datanode` **tidak jalan di Windows** (`Unrecognized option: --daemon`) — dialihkan ke `hdfs.cmd datanode` langsung di window terpisah, berhasil.
- Recovery diverifikasi: `Live datanodes (1)`, `fsck` -> `Status: HEALTHY`, `0 missing blocks`.
- Re-run batch job -> exit code 0, 2287 baris bersih, 389 pesawat unik — pulih total.
- Hasil lengkap (termasuk MTTR dan catatan tooling Windows) dicatat di `docs/design/hasil-eksperimen-resiliensi.md`.

## 2026-07-12 — Task 3: Eksperimen 2 (latensi/error ingest) — selesai, dipraktikkan manual oleh user

- Baseline `live_states`: 1358 (11:35:20).
- `ingest.py` dijalankan ~6 menit dengan `CHAOS_LATENCY_S=5` + `CHAOS_ERROR_RATE=0.2` — 1 chaos error kena (tier national), retry backoff 2s langsung sukses.
- Spark UI Structured Streaming tetap 2 query `RUNNING` sehat selama & setelah injeksi (`reports/screenshots/chaos2-streaming-survived.png`).
- `live_states` setelah injeksi: 1418 (naik wajar, tidak ada data hilang).
- Temuan sampingan (bukan akibat eksperimen ini): 1 query `FAILED` (`py4j.Py4JException`) — signature sama dengan yang terlihat sejak Task 0, bug pra-eksisting tidak terkait chaos injeksi, perlu diselidiki terpisah.
- Hasil lengkap di `docs/design/hasil-eksperimen-resiliensi.md`.

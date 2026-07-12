# M4 — Ringkasan Task 0 & Task 1 (2026-07-12)

Ringkasan eksekusi dari `docs/plans/2026-07-12-m4-validasi-resiliensi.md`. Ditulis supaya siapa pun (termasuk yang belum ikut sesinya) bisa paham apa yang sudah dikerjakan dan kenapa, tanpa perlu baca ulang seluruh percakapan.

## Konteks: apa itu M1-M4?

"M" = **Milestone integrasi** (bukan Track). Track 0/A/B/C/D dikerjakan independen lawan data contoh (fixtures); M1-M4 adalah tahap menghubungkan semua Track jadi satu pipeline nyata dan **membuktikan** benar-benar jalan dengan data asli:

| Milestone | Isinya |
|---|---|
| M1 | Jalur streaming end-to-end (ingest -> Spark Streaming -> MongoDB) |
| M2 | Jalur serving end-to-end (FastAPI + frontend, peta live) |
| M3 | Jalur batch (agregasi harian -> tab History) |
| M4 | Validasi resiliensi (3 eksperimen chaos) + persiapan presentasi |

M1-M3 sebenarnya sudah pernah diverifikasi di sesi-sesi sebelumnya (lihat `what-have-done.md`), tapi checkbox-nya di plan awal belum pernah dicentang secara resmi. M4 adalah pekerjaan yang benar-benar baru dan belum pernah dieksekusi.

## Task 0 — Smoke-check M1-M3

Tujuan: pastikan status "sudah jalan" itu masih benar HARI INI (bukan asumsi dari sesi lama), sebelum masuk ke eksperimen chaos yang butuh semua komponen jalan bareng.

**Hasil verifikasi (2026-07-12, pagi):**

- **M1 (streaming)**: file landing terbaru (`states_java_20260712T015008.json`) berumur ~47 detik saat dicek: `live_states` 1331+ dokumen di MongoDB, terus bertambah. Spark UI tab Structured Streaming menunjukkan 2 query `RUNNING` sehat.
- **M2 (serving + frontend)**: dashboard tab LIVE menampilkan ikon pesawat bergerak di atas Indonesia secara real-time (screenshot: `reports/screenshots/dashboard-live.png`).
- **M3 (batch)**: `batch_job.py` untuk tanggal hari ini exit code 0 — 1111 baris bersih dari 1517 baris mentah (reduksi wajar karena dedup `(icao24, ts)`), 243 pesawat unik. Tab HISTORY frontend terisi chart per jam + heatmap kepadatan (screenshot: `reports/screenshots/dashboard-history.png`).
- Data curated untuk 4 tanggal berturut-turut (07-09 s/d 07-12) semua utuh (`reports/hdfs-ls-curated.txt`) — konfirmasi tidak ada regresi dari bug "static partition overwrite" yang pernah ditemukan & diperbaiki sesi 2026-07-11.
- **Belum diuji ulang**: alert toast (squawk darurat 7700 via `replay.py`) — sengaja dibiarkan tidak dicentang di master plan, bisa disusulkan kapan saja.

**Temuan sampingan (bukan blocker)**: Spark UI Structured Streaming menunjukkan 1 query lama berstatus `FAILED` (`py4j.Py4JException`, mulai 08:19:54) di tab "Completed Streaming Queries" — kemungkinan sisa restart dari sesi sebelumnya, bukan dari aktivitas Task 0 ini. 2 query aktif tetap `RUNNING` normal. Perlu diperhatikan lagi kalau muncul ulang saat eksperimen chaos Task 2-4.

**Perubahan tambahan**: sekalian ditemukan & diperbaiki bug visual "radar sweep" di frontend (`it-infra-web`) — pusat rotasi sapuan radar tidak presisi di tengah layar karena (1) kotak `conic-gradient` tidak persegi mengikuti ukuran panel yang tidak persegi, dan (2) Tailwind v4 CSS `translate` utility bentrok dengan `transform` di CSS keyframe (dua-duanya menggeser elemen, menumpuk jadi -100%). Diperbaiki dengan kotak persegi berbasis `vmax` + keyframe yang cuma animasikan `rotate` (bukan `transform`). Detail lengkap ada di riwayat commit `it-infra-web` (`11bf464`) dan `it-infra` subtree.

**Commit:** `it-infra` `b57df63` (plan M4 + checkbox M1-M3 + bukti evidence), `it-infra-web` `11bf464` (fix radar-sweep, disinkron ke subtree `web/`).

## Task 1 — Chaos-injection hook di `ingest.py`

Tujuan: menyiapkan mekanisme untuk Eksperimen 2 (M4 Task 3) — suntik latensi & error rate ke proses ingest tanpa mengubah perilaku default.

**Perubahan** (`src/ingest.py`, fungsi `fetch_states`): dua env var baru, **default mati** (tidak ada risiko regresi):

- `CHAOS_LATENCY_S` — sleep (detik) sebelum tiap request ke OpenSky API
- `CHAOS_ERROR_RATE` — probabilitas (0.0-1.0) melempar error simulasi sebelum request sungguhan dikirim

Keduanya disisipkan **sebelum** loop retry yang sudah ada, supaya retry/backoff asli (2/4/8 detik) benar-benar teruji, bukan cuma didekorasi di luar.

**Verifikasi (dijalankan manual, dua arah):**

1. **Default (env var tidak di-set)** — `python src\ingest.py --once` berjalan normal persis seperti sebelumnya (`tier=java pesawat=34 kredit_dipakai=2`, dst), tidak ada baris chaos.
2. **`CHAOS_ERROR_RATE=1.0`** — setiap tier (java, national) gagal 3x berturut-turut dengan pesan `chaos: simulated ingest error (CHAOS_ERROR_RATE)`, retry backoff persis 2s/4s/8s sesuai kode asli, lalu `poll_once(...) gagal` setelah 3 percobaan — membuktikan hook benar-benar menembus jalur retry yang sudah ada.

**Commit:** `it-infra` `561d3f6`.

## Status setelah Task 0-1

Backend (HDFS, MongoDB, API, ingest, streaming) tetap jalan tanpa gangguan sepanjang Task 0-1 — tidak ada komponen yang di-restart atau di-kill (itu baru terjadi mulai Task 2). Siap lanjut ke Task 2 (Eksperimen 1: kill DataNode saat batch job).

Lihat `docs/plans/2026-07-12-m4-validasi-resiliensi.md` untuk rencana lengkap Task 2-7, dan `docs/design/hasil-eksperimen-resiliensi.md` (dibuat mulai Task 2) untuk hasil eksperimen chaos.

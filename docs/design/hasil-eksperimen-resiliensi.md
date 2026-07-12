# Hasil Eksperimen Resiliensi

## Eksperimen 1 — Kill DataNode saat batch job

- **Hipotesis:** job selesai bila replikasi >=2; pada replikasi 1 (setup lab ini) job gagal.
- **Hasil:** TERBUKTI. Setelah `taskkill /PID 18072 /F` (satu-satunya DataNode), `batch_job.py` gagal total:
  ```
  Connection refused: no further information ... /127.0.0.1:9866
  WARN DFSClient: No live nodes contain block ... after checking nodes = [DatanodeInfoWithStorage[127.0.0.1:9866,...]]
  ...
  org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times
  ```
  Spark tidak bisa membaca block raw JSON dari HDFS sama sekali karena satu-satunya salinan (replikasi 1) hilang aksesnya begitu DataNode mati — job abort sebelum menghasilkan output apa pun.
- **Rollback:** DataNode di-restart via `hdfs.cmd datanode` (dijalankan di window terpisah — lihat catatan tooling di bawah). Setelah restart: `hdfs dfsadmin -report` -> `Live datanodes (1)`; `hdfs fsck /bigdata/opensky -files -blocks` -> `Status: HEALTHY`, `0 missing blocks`.
- **Verifikasi pulih total:** `batch_job.py` dijalankan ulang untuk tanggal yang sama -> **exit code 0**, 2287 baris bersih (setelah cleaning/dedup), 389 pesawat unik, jam tersibuk 09. Dibandingkan dengan percobaan pertama (gagal total, 0 baris), ini membuktikan sistem pulih penuh tanpa data yang rusak.
- **MTTR:** ~20 detik untuk pemulihan teknis murni (dari perintah restart yang benar sampai DataNode kembali `Live` dan `fsck` HEALTHY). Namun total waktu insiden-ke-terverifikasi ~8 menit, karena upaya restart pertama gagal akibat kendala tooling (lihat catatan di bawah) — jadi MTTR realistis sangat bergantung pada operator tahu command yang benar untuk platform ini.
- **Catatan tooling (Windows-spesifik, ditemukan baru sesi ini):** `hdfs.cmd --daemon start datanode` **tidak didukung** di Hadoop-on-Windows — flag `--daemon` adalah fitur `hadoop-functions.sh` (shell script Linux), sedangkan `hdfs.cmd`/`hadoop.cmd` versi Windows tidak mengenalinya sama sekali (`Unrecognized option: --daemon`, langsung fatal exit sebelum JVM start). Tidak ada `hadoop-daemon.cmd` pun di `sbin/` (hanya versi `.sh`). Cara yang benar di Windows: jalankan `hdfs.cmd datanode` langsung (foreground) di window terpisah — pola yang sama seperti window "Ingest"/"Streaming Job" di `run_backend.ps1`.
- **Tindak lanjut:** di produksi, `dfs.replication>=3` menghilangkan single point of failure ini sepenuhnya (sudah disebut sebagai keterbatasan lab & jalur produksi di desain §6/§8, sekarang terverifikasi empiris, bukan cuma teori). Untuk operasional lab: kalau butuh restart DataNode manual di masa depan, gunakan `hdfs.cmd datanode` di window terpisah, BUKAN `--daemon` (tidak jalan di Windows).

## Eksperimen 2 — Sumber data lambat/error (inject sleep 5s + 20% error rate di ingestor)

- **Hipotesis:** streaming lanjut, latensi naik gracefully, tidak ada record hilang setelah pulih.
- **Hasil:** TERBUKTI. `ingest.py` dijalankan ~6 menit (11:37:12-11:43:27) dengan `CHAOS_LATENCY_S=5` + `CHAOS_ERROR_RATE=0.2`. Log menunjukkan 1 kegagalan simulasi kena (`fetch_states(national) attempt 1 failed: chaos: simulated ingest error`), retry backoff bawaan (2 detik) langsung berhasil di percobaan berikutnya — sisa poll lain (java tiap ~60 detik) tidak kena chaos error sama sekali di sampel ini, konsisten dengan probabilitas 20% per attempt.
- Selama & setelah periode injeksi, Spark UI Structured Streaming tetap menunjukkan 2 query `RUNNING` sehat (satu di antaranya `Avg Process/sec` 721 — ini yang mengisi `live_states`), screenshot: `reports/screenshots/chaos2-streaming-survived.png`.
- `live_states` naik dari **1358** (baseline, 11:35:20) menjadi **1418** setelah periode injeksi — bertambah wajar mengikuti trafik pesawat normal, tidak ada indikasi data hilang/macet.
- **MTTR:** N/A — tidak ada downtime nyata, sistem tetap berjalan sepanjang injeksi. Ini validasi graceful degradation, bukan recovery dari kegagalan total.
- **Temuan sampingan (bukan akibat eksperimen ini):** ada 1 query `FAILED` (`py4j.Py4JException`) di Spark UI Structured Streaming, signature error identik dengan yang sudah terlihat sejak Task 0 (sebelum eksperimen apa pun dijalankan) — jadi bukan disebabkan oleh chaos injection di ingest, kemungkinan bug pra-eksisting di salah satu query `streaming_job.py` (dugaan: query `zone_stats`/`alerts`, karena query utama `live_states` tetap sehat). **Perlu diselidiki terpisah di sesi mendatang**, di luar scope M4.
- **Tindak lanjut:** retry+exponential backoff bawaan `fetch_states` (2/4/8 detik) terbukti cukup untuk error rate transient sampai 20%. Kalau di produksi error rate API jauh lebih tinggi/persisten, perlu circuit breaker + alerting tambahan (di luar scope prototipe ini).

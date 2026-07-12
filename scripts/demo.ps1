# Skenario demo end-to-end (+-15 menit) untuk presentasi - lihat
# docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md #7.
# Skrip ini TIDAK menjalankan API/ingest/streaming sendiri (itu tugas
# run_backend.ps1/.cmd, dibiarkan di window terpisah oleh operator) - skrip
# ini murni memandu urutan tunjuk-ke-layar + menjalankan batch job di titik
# yang tepat, dengan jeda manual (Read-Host) supaya operator sempat membuka
# tab browser yang relevan sebelum lanjut.

param(
    [string]$Shell = "powershell"
)

$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Output "=== DEMO: 1/4 Start infra (HDFS + MongoDB) ==="
& (Join-Path $PSScriptRoot "start_infra.ps1")
Write-Output "Jalankan 'scripts\run_backend.cmd' (atau .ps1) di window lain sekarang - ini membuka API, Ingest, Streaming Job."
Write-Output "Tunjukkan ke audiens: file JSON baru masuk ke HDFS raw/ dan landing_stream/ tiap ~60 detik begitu ingest.py jalan."
Read-Host "Tekan Enter setelah run_backend dijalankan dan sudah menunggu ~1 menit data pertama masuk"

Write-Output "`n=== DEMO: 2/4 Structured Streaming ==="
Write-Output "Buka http://localhost:4040 tab Structured Streaming - tunjukkan query RUNNING, input rate & batch duration."
Write-Output "Buka mongosh atau shell python, jalankan: db.live_states.countDocuments() - tunjukkan angka terisi dan naik."
Read-Host "Tekan Enter untuk lanjut ke batch job"

Write-Output "`n=== DEMO: 3/4 Batch job ==="
$today = (Get-Date).ToString("yyyy-MM-dd")
try {
    & (Join-Path $PSScriptRoot "run_batch_daily.ps1") -Date $today
    Write-Output "Batch job selesai (exit code 0) untuk tanggal $today."
}
catch {
    Write-Warning "Batch job gagal untuk tanggal $today - cek output di atas sebelum lanjut demo. Error: $_"
}
Write-Output "Tunjukkan: 'hdfs dfs -ls -R /bigdata/opensky/curated' dan DAG job terakhir di Spark UI tab Jobs."
Write-Output "Catatan: kalau streaming_job.py masih jalan (port 4040 terpakai), Spark UI batch job ini otomatis geser ke port 4041 - bukan bug, satu proses Spark = satu SparkUI."
Read-Host "Tekan Enter untuk lanjut ke dashboard"

Write-Output "`n=== DEMO: 4/4 Dashboard ==="
Write-Output "Jalankan 'npm run dev' di repo it-infra-web (folder terpisah), buka http://localhost:5173."
Write-Output "Tunjukkan: peta tab LIVE bergerak tanpa refresh manual, tab HISTORY terisi untuk tanggal $today,"
Write-Output "dan (opsional) alert panel - trigger lewat src\replay.py memakai rekaman bersquawk darurat kalau perlu."
Write-Output "`n=== DEMO SELESAI ==="

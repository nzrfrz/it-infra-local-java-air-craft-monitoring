# Jalankan density_grid_hourly lewat MapReduce (Hadoop Streaming) di YARN --
# alternatif dari Spark-on-YARN yang terbukti tidak bisa jalan di mesin ini
# (bug classpath-jar Hadoop 3.3.6-Windows, lihat
# docs/design/2026-07-11-yarn-migration-design.md). Ini job MapReduce NATIVE
# (jalur kode Hadoop yang sama dengan `hadoop jar ... wordcount` yang sudah
# terbukti sukses), bukan Spark -- jadi tidak kena bug itu, DAN TERBUKTI BISA
# JALAN (lihat what-have-done.md sesi 2026-07-11 lanjutan #2). Hasil job ini
# SUPLEMEN pembanding, bukan pengganti batch_job.py (yang tetap sumber utama
# API/frontend).
#
# Usage: scripts\run_yarn_density_grid.ps1 -Date 2026-07-10
param(
    [Parameter(Mandatory = $true)][string]$Date
)

# TIDAK "Stop" -- semua logging hadoop.cmd/Hadoop Streaming (INFO/WARN) lewat
# stderr, dan PowerShell dgn ErrorActionPreference=Stop menganggap baris
# stderr apa pun dari native command sebagai NativeCommandError fatal,
# menghentikan skrip padahal exit code aslinya 0. Deteksi kegagalan asli
# lewat $LASTEXITCODE eksplisit di bawah, bukan lewat exception PowerShell.
$ErrorActionPreference = "Continue"
$repoRoot = Split-Path $PSScriptRoot -Parent
$venvPy = "D:\Coding\#bigdata\venv\Scripts\python.exe"
$streamingJar = "$env:HADOOP_HOME\share\hadoop\tools\lib\hadoop-streaming-3.3.6.jar"

# `hadoop.cmd` (dipanggil lewat `hadoop jar`) memotong argumen yang
# mengandung '=' -- bug Windows-batch yang sama dgn `hdfs dfs -put dt=...`.
# Path raw asli (raw/dt=<tanggal>) TIDAK BISA dipakai langsung sbg -input;
# staging dulu ke path tanpa '=' lewat WebHDFS murni (stage_raw.py).
$stagedInput = "/bigdata/opensky/mr-staging/$Date"
$outputPath = "/bigdata/opensky/mr-output/density_grid_hourly/$Date"

Write-Output "=== Staging raw/dt=$Date -> $stagedInput (WebHDFS) ==="
& $venvPy "$repoRoot\src\mapreduce\stage_raw.py" $Date
if ($LASTEXITCODE -ne 0) {
    Write-Warning "Staging gagal (exit $LASTEXITCODE)."
    exit $LASTEXITCODE
}

# `-file`/`-files` (opsi bawaan Hadoop Streaming utk kirim file lokal ke
# container) TIDAK BISA DIANDALKAN di Windows utk >1 file: `-file` gagal
# "not readable" (kode pengecekan eksistensi vs kode packaging jar-nya
# menganggap filesystem berbeda -- satu jalur cek pakai default FS job
# [HDFS], jalur packaging pakai file lokal literal), dan `-files a,b`
# (comma-list dalam satu argumen) terpotong jadi 2 argumen terpisah oleh
# hadoop.cmd sebelum sempat di-parse Java (dites: comma survive di
# PowerShell tapi tidak survive lewat wrapper batch-nya). Solusi yang
# TERBUKTI jalan: bundel kedua skrip jadi SATU .zip, upload ke HDFS, kirim
# lewat `-archives` (cuma butuh SATU path, tidak ada comma-list sama sekali).
$zipLocal = "$env:TEMP\it_infra_mr_scripts.zip"
if (Test-Path $zipLocal) { Remove-Item $zipLocal -Force }
Compress-Archive -Path "$repoRoot\src\mapreduce\density_grid_mapper.py", "$repoRoot\src\mapreduce\density_grid_reducer.py" -DestinationPath $zipLocal

Write-Output "=== Upload $zipLocal -> HDFS mr-scripts/ ==="
& $venvPy "$repoRoot\src\mapreduce\upload_to_hdfs.py" $zipLocal "/bigdata/opensky/mr-scripts/mr_scripts.zip"
if ($LASTEXITCODE -ne 0) {
    Write-Warning "Upload zip gagal (exit $LASTEXITCODE)."
    exit $LASTEXITCODE
}

Write-Output "=== Menghapus output lama (kalau ada): $outputPath ==="
try {
    Invoke-RestMethod -Method Delete -Uri "http://localhost:9870/webhdfs/v1$outputPath`?op=DELETE&recursive=true&user.name=Administrator" | Out-Null
} catch {
    Write-Output "(tidak ada output lama, atau sudah terhapus)"
}

Write-Output "=== Submit MapReduce job ke YARN ==="
& hadoop jar $streamingJar `
    -archives "hdfs://localhost:9000/bigdata/opensky/mr-scripts/mr_scripts.zip#mrscripts" `
    -input $stagedInput `
    -output $outputPath `
    -mapper "$venvPy mrscripts/density_grid_mapper.py" `
    -reducer "$venvPy mrscripts/density_grid_reducer.py"

if ($LASTEXITCODE -ne 0) {
    Write-Warning "Job gagal (exit $LASTEXITCODE) -- cek http://localhost:8088 untuk log aplikasi."
    exit $LASTEXITCODE
}

Write-Output "`n=== Hasil (HDFS $outputPath/part-*), 20 baris pertama ==="
& "$env:HADOOP_HOME\bin\hdfs.cmd" dfs -cat "$outputPath/part-00000" 2>$null | Select-Object -First 20

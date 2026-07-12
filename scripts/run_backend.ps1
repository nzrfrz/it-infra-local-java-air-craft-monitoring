# Menjalankan seluruh komponen backend pipeline dengan satu perintah.
# Setiap komponen jangka-panjang dibuka di window terpisah (jenis shell mengikuti
# cara script ini dipanggil - lihat parameter -Shell) supaya log masing-masing
# bisa dipantau langsung, bukan background diam-diam.
#
# batch_job.py dijalankan sekali (foreground, bukan window terpisah - selesai
# dalam hitungan puluhan detik) untuk tanggal HARI INI, supaya tab History di
# frontend selalu punya data untuk tanggal yang tampil default begitu backend
# baru dinyalakan (sebelumnya harus dijalankan manual - gampang lupa/bikin
# tab History 404). Data hari berjalan otomatis "reprocessed" tiap restart
# (overwrite per-partisi dt, idempotent) selama ingest.py sudah sempat jalan.
#
# Yang TETAP TIDAK dijalankan otomatis:
#   - src\replay.py      -> alternatif ingest.py offline (pakai rekaman), bukan dipakai bersamaan
#   - web/ (frontend)    -> `npm run dev` di repo it-infra-web terpisah
#
# Kafka (2026-07-12): menggantikan landing_stream/ sebagai transport antara
# ingest.py dan streaming_job.py -- lihat docs/design/2026-07-12-kafka-
# migration-design.md. Broker WAJIB sudah diformat sekali (kafka-storage.bat
# format, lihat docs/plans/2026-07-12-kafka-migration.md Task 1) sebelum
# script ini pertama kali dipakai -- script ini cuma START broker, tidak format.

param(
    [ValidateSet("cmd", "powershell")]
    [string]$Shell = "powershell"
)

$ErrorActionPreference = "Stop"
$root      = Split-Path -Parent $PSScriptRoot
# Resolusi venv Python, urutan prioritas (lihat README §Konfigurasi path lokal):
# 1) BIGDATA_VENV_PYTHON (override eksplisit, mis. venv dipakai bersama lintas project)
# 2) VIRTUAL_ENV (venv sudah di-activate di shell ini)
# 3) fallback venv\ self-contained di root repo (default untuk setup baru)
$venvPy = if ($env:BIGDATA_VENV_PYTHON) {
    $env:BIGDATA_VENV_PYTHON
} elseif ($env:VIRTUAL_ENV) {
    Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
} else {
    Join-Path $root "venv\Scripts\python.exe"
}
$venv      = Split-Path -Parent (Split-Path -Parent $venvPy)
$kafkaHome = $env:KAFKA_HOME  # mis. C:\Kafka\kafka_2.13-3.9.2 -- lihat System Environment Variables

if (-not (Test-Path $venvPy)) {
    Write-Error "Venv tidak ditemukan di $venvPy. Buat venv sendiri ('python -m venv venv' di root repo) atau set env var BIGDATA_VENV_PYTHON ke python.exe venv yang mau dipakai."
    exit 1
}
if (-not $kafkaHome -or -not (Test-Path $kafkaHome)) {
    Write-Error "KAFKA_HOME tidak diset/tidak valid ($kafkaHome). Cek System Environment Variables."
    exit 1
}

Write-Output "=== 1/6: HDFS + MongoDB ==="
& (Join-Path $PSScriptRoot "start_infra.ps1")

# Setiap komponen ditulis ke file launcher sungguhan (bukan satu baris "cmd /k a && b && c")
# supaya tidak kena masalah tanda kutip bersarang yang gampang salah parse oleh cmd.exe.
$launchDir = Join-Path $PSScriptRoot ".launch"
New-Item -ItemType Directory -Force -Path $launchDir | Out-Null

function Start-Component {
    # NOTE: parameter sengaja dinamai $Arguments, BUKAN $Args - $Args/$args
    # adalah automatic variable bawaan PowerShell dan akan selalu kosong di
    # dalam function kalau dipakai sebagai nama parameter (bug yang sempat
    # bikin semua komponen jalan tanpa argumen).
    param([string]$Title, [string]$Exe, [string]$Arguments)

    $safeName = $Title -replace '[^\w-]', '_'
    $exeToken = if ($Exe -match '\s') { "`"$Exe`"" } else { $Exe }

    if ($Shell -eq "cmd") {
        $file = Join-Path $launchDir "$safeName.cmd"
        @"
@echo off
title $Title
set "PATH=$venv\Scripts;%PATH%"
set "PYSPARK_PYTHON=$venvPy"
set "PYSPARK_DRIVER_PYTHON=$venvPy"
cd /d "$root"
$exeToken $Arguments
"@ | Set-Content -Path $file -Encoding ASCII
        Start-Process cmd -ArgumentList @("/k", $file)
    }
    else {
        $file = Join-Path $launchDir "$safeName.ps1"
        @"
`$Host.UI.RawUI.WindowTitle = '$Title'
`$env:PATH = '$venv\Scripts;' + `$env:PATH
`$env:PYSPARK_PYTHON = '$venvPy'
`$env:PYSPARK_DRIVER_PYTHON = '$venvPy'
Set-Location '$root'
& $exeToken $Arguments
"@ | Set-Content -Path $file -Encoding UTF8
        Start-Process powershell -ArgumentList @("-NoExit", "-ExecutionPolicy", "Bypass", "-File", $file)
    }
}

Write-Output "`n=== 2/6: Kafka broker (port 9092) [$Shell] ==="
# Server-properties KRaft ada di dalam KAFKA_HOME, bukan repo ini -- broker
# sudah harus diformat sekali sebelumnya (Task 1 kafka-migration plan).
$kafkaProps = Join-Path $kafkaHome "config\kraft\server.properties"
Start-Component -Title "Kafka Broker" -Exe (Join-Path $kafkaHome "bin\windows\kafka-server-start.bat") -Arguments "`"$kafkaProps`""
Start-Sleep -Seconds 8  # beri waktu broker siap sebelum ingest.py/streaming_job.py coba connect

Write-Output "`n=== 3/6: FastAPI (port 8000) [$Shell] ==="
# TIDAK pakai --reload: di Windows, worker uvicorn --reload jalan di bawah
# asyncio.SelectorEventLoop (bukan ProactorEventLoop), dan Selector loop
# tidak bisa spawn subprocess sama sekali -> endpoint /api/history/refresh
# (asyncio.create_subprocess_exec ke spark-submit.cmd) selalu gagal dengan
# NotImplementedError kalau --reload aktif. Restart manual window ini kalau
# edit api/main.py.
Start-Component -Title "API - uvicorn" -Exe $venvPy -Arguments "-m uvicorn api.main:app --host 0.0.0.0 --port 8000"

Write-Output "=== 4/6: ingest.py (poller OpenSky) [$Shell] ==="
Start-Component -Title "Ingest" -Exe $venvPy -Arguments "src\ingest.py"

Write-Output "=== 5/6: streaming_job.py (Spark Structured Streaming, Kafka source) [$Shell] ==="
# --packages WAJIB -- connector Kafka bukan bagian default Spark, di-download
# via Ivy (sekali, lalu ter-cache di ~/.ivy2). Tanpa ini: ClassNotFoundException
# org.apache.spark.sql.kafka010.KafkaSourceProvider.
Start-Component -Title "Streaming Job" -Exe "spark-submit" -Arguments "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src\streaming_job.py"

Write-Output "=== 6/6: batch_job.py (agregasi harian, tanggal hari ini) ==="
$env:PATH = "$venv\Scripts;" + $env:PATH
$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy
$today = (Get-Date).ToString("yyyy-MM-dd")
try {
    & spark-submit "src\batch_job.py" --date $today
}
catch {
    Write-Warning "batch_job.py gagal untuk tanggal $today - tab History mungkin masih 404. Jalankan manual: scripts\run_batch_daily.ps1 -Date $today"
}

Write-Output "`nSemua komponen jangka-panjang berjalan di window $Shell terpisah."
Write-Output "Manual/opsional: src\replay.py (offline demo), frontend (npm run dev di it-infra-web)."
Write-Output "batch_job.py utk tanggal lain: scripts\run_batch_daily.ps1 -Date YYYY-MM-DD"

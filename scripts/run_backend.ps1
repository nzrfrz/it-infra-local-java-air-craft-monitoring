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

param(
    [ValidateSet("cmd", "powershell")]
    [string]$Shell = "powershell"
)

$ErrorActionPreference = "Stop"
$root   = Split-Path -Parent $PSScriptRoot
$venv   = "D:\Coding\#bigdata\venv"
$venvPy = Join-Path $venv "Scripts\python.exe"

if (-not (Test-Path $venvPy)) {
    Write-Error "Venv bersama tidak ditemukan di $venvPy. Cek lokasi venv #bigdata."
    exit 1
}

Write-Output "=== 1/4: HDFS + MongoDB ==="
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

Write-Output "`n=== 2/4: FastAPI (port 8000) [$Shell] ==="
Start-Component -Title "API - uvicorn" -Exe $venvPy -Arguments "-m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000"

Write-Output "=== 3/4: ingest.py (poller OpenSky) [$Shell] ==="
Start-Component -Title "Ingest" -Exe $venvPy -Arguments "src\ingest.py"

Write-Output "=== 4/5: streaming_job.py (Spark Structured Streaming) [$Shell] ==="
Start-Component -Title "Streaming Job" -Exe "spark-submit" -Arguments "src\streaming_job.py"

Write-Output "=== 5/5: batch_job.py (agregasi harian, tanggal hari ini) ==="
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

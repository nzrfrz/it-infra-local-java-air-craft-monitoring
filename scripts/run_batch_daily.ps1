# Jalankan batch_job.py untuk data kemarin (default). Dipakai manual atau
# didaftarkan ke Windows Task Scheduler (mis. jam 01:00) untuk jalur produksi.
#
# Registrasi manual (jalankan sekali sebagai admin, sesuaikan path ke lokasi clone repo Anda):
#   schtasks /Create /TN "OpenSky Batch Daily" /TR "powershell.exe -File <path-ke-repo>\scripts\run_batch_daily.ps1" /SC DAILY /ST 01:00

param(
    [string]$Date = $null
)

$ErrorActionPreference = "Stop"
$root   = Split-Path -Parent $PSScriptRoot
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
$venv = Split-Path -Parent (Split-Path -Parent $venvPy)

if (-not (Test-Path $venvPy)) {
    Write-Error "Venv tidak ditemukan di $venvPy. Buat venv sendiri ('python -m venv venv' di root repo) atau set env var BIGDATA_VENV_PYTHON ke python.exe venv yang mau dipakai."
    exit 1
}

Set-Location $root

# PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON harus di-set SEBELUM spark-submit
# dipanggil, bukan dari dalam batch_job.py - proses driver Spark sudah keburu
# start pakai "python" polos dari PATH (lihat what-have-done.md, bug operasional
# yang sama juga kena run_backend.ps1 utk streaming_job.py).
$env:PATH = "$venv\Scripts;" + $env:PATH
$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy

$sparkArgs = @("src\batch_job.py")
if ($Date) {
    $sparkArgs += @("--date", $Date)
}

& spark-submit @sparkArgs

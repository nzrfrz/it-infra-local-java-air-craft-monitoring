# Jalankan batch_job.py untuk data kemarin (default). Dipakai manual atau
# didaftarkan ke Windows Task Scheduler (mis. jam 01:00) untuk jalur produksi.
#
# Registrasi manual (jalankan sekali sebagai admin, sesuaikan path):
#   schtasks /Create /TN "OpenSky Batch Daily" /TR "powershell.exe -File D:\Coding\#bigdata\it-infra\scripts\run_batch_daily.ps1" /SC DAILY /ST 01:00

param(
    [string]$Date = $null
)

$ErrorActionPreference = "Stop"
$root   = Split-Path -Parent $PSScriptRoot
$venv   = "D:\Coding\#bigdata\venv"
$venvPy = Join-Path $venv "Scripts\python.exe"

if (-not (Test-Path $venvPy)) {
    Write-Error "Venv bersama tidak ditemukan di $venvPy. Cek lokasi venv #bigdata."
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

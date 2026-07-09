# Jalankan batch_job.py untuk data kemarin (default). Dipakai manual atau
# didaftarkan ke Windows Task Scheduler (mis. jam 01:00) untuk jalur produksi.
#
# Registrasi manual (jalankan sekali sebagai admin, sesuaikan path):
#   schtasks /Create /TN "OpenSky Batch Daily" /TR "powershell.exe -File D:\Coding\#bigdata\it-infra\scripts\run_batch_daily.ps1" /SC DAILY /ST 01:00

param(
    [string]$Date = $null
)

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$args = @("src\batch_job.py")
if ($Date) {
    $args += @("--date", $Date)
}

& ".\venv\Scripts\python.exe" @args

# Start HDFS (NameNode + DataNode) dan verifikasi MongoDB replica set.
# Prasyarat satu kali (sudah dilakukan): HDFS namenode sudah diformat,
# MongoDB service sudah dikonfigurasi dengan replication.replSetName: rs0
# di mongod.cfg dan rs.initiate() sudah dijalankan.

Write-Output "=== Starting HDFS ==="
& "$env:HADOOP_HOME\sbin\start-dfs.cmd"
Start-Sleep -Seconds 10

# NameNode start di safe mode sampai DataNode selesai kirim block report -
# menulis (mkdirs/create) ditolak selama itu. Tunggu sampai benar-benar OFF
# sebelum lanjut, supaya ingest.py/streaming_job.py yang jalan setelah script
# ini tidak kena 403/mkdir gagal gara-gara race condition.
Write-Output "Menunggu HDFS keluar dari safe mode..."
$maxWait = 60
$waited = 0
do {
    $safemode = & "$env:HADOOP_HOME\bin\hdfs.cmd" dfsadmin -safemode get 2>&1
    if ($safemode -match "OFF") { break }
    Start-Sleep -Seconds 3
    $waited += 3
} while ($waited -lt $maxWait)
Write-Output "Safemode: $safemode (menunggu ${waited}s)"
if ($safemode -notmatch "OFF") {
    Write-Warning "HDFS masih safe mode setelah ${maxWait}s - cek DataNode (`hdfs dfsadmin -report`) sebelum lanjut jalankan ingest/streaming."
}

& "$env:HADOOP_HOME\bin\hdfs.cmd" dfs -mkdir -p /bigdata/opensky/raw /bigdata/opensky/curated /bigdata/opensky/checkpoints
& "$env:HADOOP_HOME\bin\hdfs.cmd" dfs -ls /bigdata/opensky

Write-Output "`n=== Checking MongoDB service ==="
$svc = Get-Service -Name MongoDB -ErrorAction SilentlyContinue
if ($svc -and $svc.Status -ne "Running") {
    Start-Service -Name MongoDB
}
Get-Service -Name MongoDB

Write-Output "`n=== Verifying MongoDB replica set ==="
# Resolve python secara eksplisit - relying on bare "python" bisa kepilih
# system/store install ahead of venv\Scripts on PATH. Urutan resolusi:
# 1) BIGDATA_VENV_PYTHON (override eksplisit, dipakai kalau venv dipakai
#    bersama lintas project - lihat README §Konfigurasi path lokal)
# 2) VIRTUAL_ENV (venv sudah di-activate di shell ini)
# 3) fallback venv\ self-contained di root repo (default untuk setup baru)
$repoRoot = Split-Path -Parent $PSScriptRoot
$py = if ($env:BIGDATA_VENV_PYTHON) {
    $env:BIGDATA_VENV_PYTHON
} elseif ($env:VIRTUAL_ENV) {
    Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
} else {
    Join-Path $repoRoot "venv\Scripts\python.exe"
}
& $py -c "from pymongo import MongoClient; c = MongoClient('mongodb://localhost:27017/?replicaSet=rs0&directConnection=true', serverSelectionTimeoutMS=5000); s = c.admin.command('replSetGetStatus'); print('replica set:', s['set'], '| state:', s['myState'])"

Write-Output "`n=== Local folders ==="
$dataRoot = Join-Path $PSScriptRoot "..\landing-stream-recording"
New-Item -ItemType Directory -Force -Path (Join-Path $dataRoot "landing_stream") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $dataRoot "recordings") | Out-Null
Write-Output "landing_stream & recordings ready under $dataRoot"

Write-Output "`n=== Infra status: OK ==="

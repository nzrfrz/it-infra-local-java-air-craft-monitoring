# Start HDFS (NameNode + DataNode) dan verifikasi MongoDB replica set.
# Prasyarat satu kali (sudah dilakukan): HDFS namenode sudah diformat,
# MongoDB service sudah dikonfigurasi dengan replication.replSetName: rs0
# di mongod.cfg dan rs.initiate() sudah dijalankan.

Write-Output "=== Starting HDFS ==="
& "$env:HADOOP_HOME\sbin\start-dfs.cmd"
Start-Sleep -Seconds 10

$safemode = & "$env:HADOOP_HOME\bin\hdfs.cmd" dfsadmin -safemode get 2>&1
Write-Output "Safemode: $safemode"

& "$env:HADOOP_HOME\bin\hdfs.cmd" dfs -mkdir -p /bigdata/opensky/raw /bigdata/opensky/curated /bigdata/opensky/checkpoints
& "$env:HADOOP_HOME\bin\hdfs.cmd" dfs -ls /bigdata/opensky

Write-Output "`n=== Checking MongoDB service ==="
$svc = Get-Service -Name MongoDB -ErrorAction SilentlyContinue
if ($svc -and $svc.Status -ne "Running") {
    Start-Service -Name MongoDB
}
Get-Service -Name MongoDB

Write-Output "`n=== Verifying MongoDB replica set ==="
python -c "from pymongo import MongoClient; c = MongoClient('mongodb://localhost:27017/?replicaSet=rs0&directConnection=true', serverSelectionTimeoutMS=5000); s = c.admin.command('replSetGetStatus'); print('replica set:', s['set'], '| state:', s['myState'])"

Write-Output "`n=== Local folders ==="
New-Item -ItemType Directory -Force -Path "D:\bigdata\landing_stream" | Out-Null
New-Item -ItemType Directory -Force -Path "D:\bigdata\recordings" | Out-Null
Write-Output "landing_stream & recordings ready under D:\bigdata\"

Write-Output "`n=== Infra status: OK ==="

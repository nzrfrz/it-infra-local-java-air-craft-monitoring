"""Baca dataset Parquet curated dari HDFS lewat WebHDFS REST API.

Konsisten dengan workaround yang sudah dipakai `src/ingest.py`: Hadoop-on-Windows
tidak selalu punya libhdfs native yang dibutuhkan pyarrow.fs.HadoopFileSystem,
jadi baca lewat WebHDFS (LISTSTATUS + OPEN) lalu parse bytes-nya dengan
pyarrow.parquet, bukan lewat filesystem HDFS native.
"""
import io
import socket
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import urllib3.util.connection as _urllib3_conn

# WebHDFS redirect OPEN ke datanode pakai hostname mesin (mis. "WIN-XXXX"), yang
# resolve ke alamat IPv6 link-local *dan* IPv4. requests/urllib3 mencoba IPv6
# duluan dan butuh beberapa detik gagal sebelum fallback ke IPv4 -> tiap
# pembacaan file jadi lambat (~4 dtk/file, x ratusan file = menit). Paksa
# resolusi IPv4-only supaya tidak kena delay itu.
_urllib3_conn.allowed_gai_family = lambda: socket.AF_INET

_MAX_PARALLEL_READS = 16

_WEBHDFS_BASE = "http://localhost:9870/webhdfs/v1"


class ParquetPartitionNotFound(Exception):
    """Partisi dt=<tanggal> belum ada di curated zone (batch job belum jalan)."""


def _list_status(hdfs_path):
    resp = requests.get(f"{_WEBHDFS_BASE}{hdfs_path}", params={"op": "LISTSTATUS"}, timeout=15)
    if resp.status_code == 404:
        raise ParquetPartitionNotFound(hdfs_path)
    resp.raise_for_status()
    return resp.json()["FileStatuses"]["FileStatus"]


def _read_file_bytes(hdfs_path):
    resp = requests.get(f"{_WEBHDFS_BASE}{hdfs_path}", params={"op": "OPEN"}, timeout=30)
    resp.raise_for_status()
    return resp.content


def read_partition(hdfs_base: str, dataset: str, date_str: str) -> pa.Table:
    """Baca semua part-*.parquet di {hdfs_base}/curated/{dataset}/dt={date_str}/
    dan gabung jadi satu pyarrow.Table."""
    hdfs_root = urlparse(hdfs_base).path or "/"
    partition_dir = f"{hdfs_root}/curated/{dataset}/dt={date_str}"

    statuses = _list_status(partition_dir)
    part_files = [
        f"{partition_dir}/{s['pathSuffix']}"
        for s in statuses
        if s["type"] == "FILE" and s["pathSuffix"].endswith(".parquet")
    ]
    if not part_files:
        raise ParquetPartitionNotFound(partition_dir)

    # Batch job bisa menulis puluhan/ratusan part file kecil (default paralelisme
    # Spark) untuk data yang volumenya kecil per hari -- baca paralel lewat thread
    # pool supaya latensi WebHDFS per file (redirect ke datanode) tidak terjumlah
    # secara sekuensial.
    with ThreadPoolExecutor(max_workers=min(_MAX_PARALLEL_READS, len(part_files))) as pool:
        blobs = list(pool.map(_read_file_bytes, part_files))
    tables = [pq.read_table(io.BytesIO(b)) for b in blobs]
    return pa.concat_tables(tables) if len(tables) > 1 else tables[0]

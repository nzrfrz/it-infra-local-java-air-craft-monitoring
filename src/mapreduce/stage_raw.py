"""Salin file JSON mentah dari HDFS raw/dt=<tanggal>/ ke direktori staging
TANPA karakter '=' di path (mis. raw/dt=2026-07-10/ -> mr-staging/2026-07-10/)
lewat WebHDFS REST murni -- `hadoop.cmd`/`hdfs.cmd` di Windows diketahui
memotong argumen command-line yang mengandung '=' (bug yang sama dicatat di
what-have-done.md utk `hdfs dfs -put dt=<tanggal>`), jadi job MapReduce
(Hadoop Streaming) tidak bisa langsung pakai path asli raw/dt=<tanggal>
sebagai -input/-output.

Usage: python stage_raw.py <tanggal YYYY-MM-DD>
"""
import sys

import requests

_WEBHDFS_BASE = "http://localhost:9870/webhdfs/v1"
_USER = "Administrator"


def _list(path):
    resp = requests.get(f"{_WEBHDFS_BASE}{path}", params={"op": "LISTSTATUS", "user.name": _USER}, timeout=30)
    resp.raise_for_status()
    return resp.json()["FileStatuses"]["FileStatus"]


def _delete(path):
    requests.delete(
        f"{_WEBHDFS_BASE}{path}",
        params={"op": "DELETE", "recursive": "true", "user.name": _USER},
        timeout=30,
    )


def _copy_file(src_path, dst_path):
    resp = requests.get(f"{_WEBHDFS_BASE}{src_path}", params={"op": "OPEN", "user.name": _USER}, timeout=30)
    resp.raise_for_status()
    content = resp.content

    create_resp = requests.put(
        f"{_WEBHDFS_BASE}{dst_path}",
        params={"op": "CREATE", "overwrite": "true", "user.name": _USER},
        allow_redirects=False,
        timeout=15,
    )
    if create_resp.status_code != 307:
        create_resp.raise_for_status()
        raise RuntimeError(
            f"WebHDFS CREATE tidak me-redirect utk {dst_path}: {create_resp.status_code} {create_resp.text}"
        )
    datanode_url = create_resp.headers["Location"]
    put_resp = requests.put(datanode_url, data=content, timeout=60)
    put_resp.raise_for_status()


def main():
    date_str = sys.argv[1]
    src_dir = f"/bigdata/opensky/raw/dt={date_str}"
    dst_dir = f"/bigdata/opensky/mr-staging/{date_str}"

    _delete(dst_dir)

    files = _list(src_dir)
    count = 0
    for f in files:
        if f["type"] != "FILE":
            continue
        name = f["pathSuffix"]
        _copy_file(f"{src_dir}/{name}", f"{dst_dir}/{name}")
        count += 1
    print(f"[stage_raw] disalin {count} file dari {src_dir} -> {dst_dir}")


if __name__ == "__main__":
    main()

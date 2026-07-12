"""Upload 1 file lokal ke HDFS lewat WebHDFS REST murni (bukan `hdfs dfs -put`,
supaya konsisten dengan pola workaround '=' yang sudah dipakai di project ini
dan menghindari ketergantungan pada CLI wrapper Windows sama sekali).

Usage: python upload_to_hdfs.py <local_path> <hdfs_path>
"""
import sys

import requests

_WEBHDFS_BASE = "http://localhost:9870/webhdfs/v1"
_USER = "Administrator"


def main():
    local_path, hdfs_path = sys.argv[1], sys.argv[2]

    # Langkah 1: minta NameNode buatkan file & redirect ke DataNode tujuan
    # (protokol WebHDFS 2-langkah: 307 dulu, baru upload byte sungguhan).
    create_resp = requests.put(
        f"{_WEBHDFS_BASE}{hdfs_path}",
        params={"op": "CREATE", "overwrite": "true", "user.name": _USER},
        allow_redirects=False,
        timeout=15,
    )
    if create_resp.status_code != 307:
        create_resp.raise_for_status()
        raise RuntimeError(
            f"WebHDFS CREATE tidak me-redirect utk {hdfs_path}: {create_resp.status_code} {create_resp.text}"
        )
    datanode_url = create_resp.headers["Location"]
    # Langkah 2: upload isi file sungguhan ke DataNode.
    with open(local_path, "rb") as f:
        put_resp = requests.put(datanode_url, data=f.read(), timeout=60)
    put_resp.raise_for_status()
    print(f"[upload_to_hdfs] {local_path} -> {hdfs_path}")


if __name__ == "__main__":
    main()

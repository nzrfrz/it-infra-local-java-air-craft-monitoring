"""Poller OpenSky 2-tier -> HDFS raw zone (arsip) + Kafka topic opensky.states
(dikonsumsi Structured Streaming, lihat docs/design/2026-07-12-kafka-migration-design.md)
+ recordings/ (arsip lokal untuk replay.py).

Usage: python src/ingest.py [--config config/config.yaml] [--once]
"""
import argparse
import json
import logging
import os
import random
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Endpoint WebHDFS (bukan HDFS native client) -- lihat _put_to_hdfs() di bawah
# untuk alasannya (bug hdfs.cmd di Windows dengan path yang mengandung '=').
_WEBHDFS_BASE = "http://localhost:9870/webhdfs/v1"

# Supaya "from common import ..." bisa jalan walau script ini dipanggil dari
# direktori lain (mis. dari scripts/*.ps1 yang cd ke root repo dulu).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import flatten, load_config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest")

# Cache token OAuth2 di memori proses -- OpenSky token berlaku ~30 menit,
# jadi tidak perlu request token baru di setiap polling (hemat request & lebih cepat).
_token_cache = {"access_token": None, "expires_at": 0}


def get_token(cfg):
    """OAuth2 client-credentials ke OpenSky, cache token sampai expiry."""
    now = time.time()
    # Masih ada token yang valid (dengan buffer 30 dtk sebelum expiry asli) -> pakai lagi.
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 30:
        return _token_cache["access_token"]

    # Token belum ada / sudah kedaluwarsa -> minta token baru ke OpenSky.
    resp = requests.post(
        cfg["opensky"]["token_url"],
        data={
            "grant_type": "client_credentials",
            "client_id": cfg["opensky"]["client_id"],
            "client_secret": cfg["opensky"]["client_secret"],
        },
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    _token_cache["access_token"] = payload["access_token"]
    _token_cache["expires_at"] = now + payload.get("expires_in", 1800)
    return _token_cache["access_token"]


def credits_for_bbox(bbox):
    """Hitung biaya kredit OpenSky berdasarkan luas bounding box (deg^2) --
    aturan resmi OpenSky: makin luas area yang diminta, makin mahal per
    panggilan. Dipakai untuk melacak sisa kuota harian (4000 kredit/hari)."""
    area = (bbox["lamax"] - bbox["lamin"]) * (bbox["lomax"] - bbox["lomin"])
    if area <= 25:
        return 1
    if area <= 100:
        return 2
    if area <= 400:
        return 3
    return 4


def fetch_states(cfg, tier, max_retries=3):
    """GET /states/all dengan bbox tier; retry exponential backoff (2/4/8s).

    Chaos hooks (env var, default mati - lihat docs/plans/2026-07-12-m4-validasi-resiliensi.md
    Eksperimen 2): CHAOS_LATENCY_S menyisipkan sleep sebelum tiap request,
    CHAOS_ERROR_RATE melempar error simulasi dengan probabilitas tsb sebelum
    request sungguhan dikirim, supaya retry/backoff asli di bawah ini teruji."""
    chaos_latency = float(os.environ.get("CHAOS_LATENCY_S", "0"))
    chaos_error_rate = float(os.environ.get("CHAOS_ERROR_RATE", "0"))
    bbox = cfg["opensky"]["tiers"][tier]
    params = {
        "lamin": bbox["lamin"],
        "lamax": bbox["lamax"],
        "lomin": bbox["lomin"],
        "lomax": bbox["lomax"],
    }
    last_err = None
    # Sampai max_retries kali percobaan; tiap gagal, tunggu makin lama
    # sebelum coba lagi (exponential backoff: 2 dtk, lalu 4 dtk, lalu 8 dtk).
    for attempt in range(max_retries):
        try:
            if chaos_latency > 0:
                time.sleep(chaos_latency)
            if chaos_error_rate > 0 and random.random() < chaos_error_rate:
                raise RuntimeError("chaos: simulated ingest error (CHAOS_ERROR_RATE)")
            token = get_token(cfg)
            resp = requests.get(
                cfg["opensky"]["api_url"],
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                timeout=20,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001 - network errors of many shapes
            last_err = exc
            wait = 2 ** (attempt + 1)  # 2, 4, 8 detik
            log.warning("fetch_states(%s) attempt %d failed: %s; retry in %ds", tier, attempt + 1, exc, wait)
            time.sleep(wait)
    # Semua percobaan gagal -> lempar exception ke pemanggil (poll_once akan
    # menangkapnya, log error, dan lanjut ke tier berikutnya tanpa crash total).
    raise RuntimeError(f"fetch_states({tier}) failed after {max_retries} attempts") from last_err


def _write_ndjson_atomic(rows, dest_dir: Path, filename: str):
    """Tulis file lalu rename, bukan tulis langsung ke nama final -- supaya
    Structured Streaming (yang memantau folder ini) tidak pernah membaca
    file yang sedang setengah ditulis. `rename` di filesystem lokal bersifat
    atomik, jadi file cuma "muncul" begitu isinya sudah lengkap."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_dir / f"{filename}.tmp"
    final_path = dest_dir / filename
    with open(tmp_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    tmp_path.rename(final_path)
    return final_path


def _put_to_hdfs(local_path: Path, hdfs_dir: str):
    """Upload via WebHDFS REST (bukan `hdfs dfs` CLI): hdfs.cmd di Windows
    memotong argumen path yang mengandung '=' (mis. 'dt=2026-07-09'), jadi
    partisi Hive-style gagal dibuat lewat CLI. WebHDFS tidak kena bug ini."""
    hdfs_path = f"{hdfs_dir}/{local_path.name}"
    create_url = f"{_WEBHDFS_BASE}{hdfs_path}"
    # Langkah 1: minta NameNode buatkan file & redirect ke DataNode yang
    # benar-benar menyimpan datanya (WebHDFS protokol 2 langkah: 307 dulu,
    # baru upload ke Location yang dikembalikan).
    resp = requests.put(
        create_url,
        params={"op": "CREATE", "overwrite": "true", "user.name": "Administrator"},
        allow_redirects=False,
        timeout=15,
    )
    if resp.status_code != 307:
        resp.raise_for_status()
        raise RuntimeError(f"WebHDFS CREATE tidak me-redirect (status {resp.status_code}): {resp.text}")

    # Langkah 2: upload isi file sungguhan ke URL DataNode dari header Location.
    datanode_url = resp.headers["Location"]
    with open(local_path, "rb") as f:
        resp2 = requests.put(datanode_url, data=f.read(), timeout=30)
    resp2.raise_for_status()


def _send_to_kafka(producer, topic, flat_rows):
    """Publish tiap baris (1 pesawat) sebagai 1 pesan Kafka -- menggantikan
    tulis ke landing_stream/ (lihat docs/design/2026-07-12-kafka-migration-design.md).
    Key = icao24, supaya semua event pesawat yang sama selalu ke partition yang
    sama (ordering per-pesawat terjaga). acks="1" (bukan "all") karena
    replication-factor topic ini cuma 1 -- tidak ada replica lain untuk di-ack."""
    for row in flat_rows:
        producer.send(
            topic,
            key=row["icao24"].encode("utf-8"),
            value=json.dumps(row).encode("utf-8"),
        )
    # flush blocking sampai semua pesan micro-batch ini terkirim/gagal --
    # supaya kegagalan kirim ketahuan di sini (di-log oleh pemanggil), bukan
    # diam-diam hilang di background callback.
    producer.flush(timeout=10)


def write_outputs(raw, flat_rows, tier, cfg, ts, producer):
    """(1) raw JSON -> HDFS raw/dt=YYYY-MM-DD/; (2) tiap baris -> Kafka topic
    (dikonsumsi streaming_job.py); (3) salinan NDJSON -> recordings/."""
    dt_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    filename = f"states_{tier}_{ts_str}.json"

    # (1) Simpan respons MENTAH (belum di-flatten) ke HDFS sebagai arsip
    # permanen -- ini yang dibaca ulang oleh batch_job.py setiap hari.
    with tempfile.TemporaryDirectory() as tmp:
        raw_path = Path(tmp) / filename
        raw_path.write_text(json.dumps(raw), encoding="utf-8")
        hdfs_path_prefix = urlparse(cfg["hdfs"]["base"]).path  # buang skema hdfs://host:port
        hdfs_dir = f"{hdfs_path_prefix}/raw/dt={dt_str}"
        try:
            _put_to_hdfs(raw_path, hdfs_dir)
        except Exception as exc:  # noqa: BLE001
            # Gagal tulis ke HDFS TIDAK menghentikan proses ingest -- data tetap
            # sempat dikirim ke Kafka/recordings di bawah, jadi streaming tetap
            # dapat data walau arsip HDFS-nya sempat bolong.
            log.error("gagal menulis raw ke HDFS: %s", exc)

    # (2) Publish baris yang sudah DI-FLATTEN (per pesawat) ke Kafka topic --
    # ini yang dikonsumsi streaming_job.py sebagai source. Gagal kirim TIDAK
    # menghentikan proses ingest (pola sama dengan gagal-HDFS di atas).
    try:
        _send_to_kafka(producer, cfg["kafka"]["topic"], flat_rows)
    except KafkaError as exc:
        log.error("gagal publish ke Kafka: %s", exc)

    # (3) Salinan yang sama juga disimpan ke recordings/ -- arsip lokal untuk
    # replay.py (demo offline kalau internet/kuota OpenSky bermasalah).
    recordings_dir = Path(cfg["paths"]["recordings"])
    _write_ndjson_atomic(flat_rows, recordings_dir, filename)


def poll_once(cfg, tier, producer):
    """Satu siklus lengkap untuk satu tier: ambil data -> flatten -> tulis ke
    3 tujuan (HDFS raw, Kafka, recordings) -> hitung kredit terpakai."""
    now = time.time()
    raw = fetch_states(cfg, tier)
    flat_rows = flatten(raw, tier, fetched_at=int(now))
    write_outputs(raw, flat_rows, tier, cfg, ts=int(now), producer=producer)
    credits = credits_for_bbox(cfg["opensky"]["tiers"][tier])
    log.info("tier=%s pesawat=%d kredit_dipakai=%d", tier, len(flat_rows), credits)
    return credits


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--once", action="store_true", help="jalankan satu putaran per tier lalu keluar")
    args = parser.parse_args()

    cfg = load_config(args.config)
    tiers = cfg["opensky"]["tiers"]
    # next_run[tier]: kapan tier itu boleh di-poll lagi (epoch time). Dimulai
    # 0 supaya SEMUA tier langsung di-poll begitu proses start, bukan menunggu
    # interval_s pertama kali.
    next_run = {tier: 0.0 for tier in tiers}
    credits_today = 0
    credits_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Producer dibuat SEKALI di awal (bukan per-poll) -- KafkaProducer sudah
    # thread-safe & connection-pooled secara internal, bikin ulang tiap poll
    # cuma nambah overhead handshake ke broker tanpa manfaat.
    #
    # api_version dipaksa eksplisit (bukan auto-negotiate) supaya konsisten
    # dengan versi broker yang diuji (Kafka 3.9.2 KRaft, lihat
    # docs/design/2026-07-12-kafka-migration-design.md). acks=1 (int, BUKAN
    # string "1" -- kafka-python meng-encode field ini langsung sebagai
    # integer protokol, string gagal di-pack dengan error yang membingungkan
    # "required argument is not an integer").
    producer = KafkaProducer(
        bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
        acks=1,
        api_version=(2, 6, 0),
    )

    log.info("ingest.py dimulai, tiers=%s", list(tiers.keys()))

    # Loop utama: tiap tier (mis. "java" tiap 60 dtk, "national" tiap 15 mnt)
    # dicek independen apakah sudah waktunya di-poll lagi -- bukan satu
    # interval global, karena tiap tier punya kuota/frekuensi berbeda (§2 desain).
    while True:
        now = time.time()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != credits_date:
            # Ganti hari (UTC) -> reset penghitung kredit harian.
            credits_date = today
            credits_today = 0

        for tier, bbox in tiers.items():
            if now >= next_run[tier]:
                try:
                    credits_today += poll_once(cfg, tier, producer)
                    log.info("kredit terpakai hari ini: %d/4000", credits_today)
                except Exception as exc:  # noqa: BLE001
                    # Satu tier gagal (mis. fetch_states habis retry) tidak
                    # menghentikan tier lain maupun loop utama -- dicatat lalu lanjut.
                    log.error("poll_once(%s) gagal: %s", tier, exc)
                next_run[tier] = now + bbox["interval_s"]

        if args.once:
            break
        time.sleep(1)  # cek ulang tiap 1 detik apakah ada tier yang jatuh tempo


if __name__ == "__main__":
    main()

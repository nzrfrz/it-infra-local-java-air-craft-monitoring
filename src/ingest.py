"""Poller OpenSky 2-tier -> HDFS raw zone (arsip) + landing_stream/ (NDJSON,
dikonsumsi Structured Streaming) + recordings/ (arsip lokal untuk replay.py).

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

_WEBHDFS_BASE = "http://localhost:9870/webhdfs/v1"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import flatten, load_config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest")

_token_cache = {"access_token": None, "expires_at": 0}


def get_token(cfg):
    """OAuth2 client-credentials ke OpenSky, cache token sampai expiry."""
    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 30:
        return _token_cache["access_token"]

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
            wait = 2 ** (attempt + 1)
            log.warning("fetch_states(%s) attempt %d failed: %s; retry in %ds", tier, attempt + 1, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"fetch_states({tier}) failed after {max_retries} attempts") from last_err


def _write_ndjson_atomic(rows, dest_dir: Path, filename: str):
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
    resp = requests.put(
        create_url,
        params={"op": "CREATE", "overwrite": "true", "user.name": "Administrator"},
        allow_redirects=False,
        timeout=15,
    )
    if resp.status_code != 307:
        resp.raise_for_status()
        raise RuntimeError(f"WebHDFS CREATE tidak me-redirect (status {resp.status_code}): {resp.text}")

    datanode_url = resp.headers["Location"]
    with open(local_path, "rb") as f:
        resp2 = requests.put(datanode_url, data=f.read(), timeout=30)
    resp2.raise_for_status()


def write_outputs(raw, flat_rows, tier, cfg, ts):
    """(1) raw JSON -> HDFS raw/dt=YYYY-MM-DD/; (2) NDJSON -> landing_stream/
    (tmp+rename); (3) salinan NDJSON -> recordings/."""
    dt_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    filename = f"states_{tier}_{ts_str}.json"

    with tempfile.TemporaryDirectory() as tmp:
        raw_path = Path(tmp) / filename
        raw_path.write_text(json.dumps(raw), encoding="utf-8")
        hdfs_path_prefix = urlparse(cfg["hdfs"]["base"]).path  # buang skema hdfs://host:port
        hdfs_dir = f"{hdfs_path_prefix}/raw/dt={dt_str}"
        try:
            _put_to_hdfs(raw_path, hdfs_dir)
        except Exception as exc:  # noqa: BLE001
            log.error("gagal menulis raw ke HDFS: %s", exc)

    landing_dir = Path(cfg["paths"]["landing_stream"])
    _write_ndjson_atomic(flat_rows, landing_dir, filename)

    recordings_dir = Path(cfg["paths"]["recordings"])
    _write_ndjson_atomic(flat_rows, recordings_dir, filename)


def poll_once(cfg, tier):
    now = time.time()
    raw = fetch_states(cfg, tier)
    flat_rows = flatten(raw, tier, fetched_at=int(now))
    write_outputs(raw, flat_rows, tier, cfg, ts=int(now))
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
    next_run = {tier: 0.0 for tier in tiers}
    credits_today = 0
    credits_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    log.info("ingest.py dimulai, tiers=%s", list(tiers.keys()))

    while True:
        now = time.time()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != credits_date:
            credits_date = today
            credits_today = 0

        for tier, bbox in tiers.items():
            if now >= next_run[tier]:
                try:
                    credits_today += poll_once(cfg, tier)
                    log.info("kredit terpakai hari ini: %d/4000", credits_today)
                except Exception as exc:  # noqa: BLE001
                    log.error("poll_once(%s) gagal: %s", tier, exc)
                next_run[tier] = now + bbox["interval_s"]

        if args.once:
            break
        time.sleep(1)


if __name__ == "__main__":
    main()

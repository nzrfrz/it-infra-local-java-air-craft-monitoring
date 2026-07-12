"""Fallback demo offline: putar ulang rekaman NDJSON di recordings/ ke
landing_stream/ dengan jeda waktu asli (atau dipercepat via --speed).

Berguna bila demo berlangsung saat koneksi internet bermasalah atau kuota
OpenSky habis.

Usage: python src/replay.py [--config config/config.yaml] [--speed 5]
"""
import argparse
import re
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import load_config  # noqa: E402

# Sama seperti batch_job.py -- nama file menyimpan tier & timestamp snapshot.
_FILENAME_RE = re.compile(r"^states_(?P<tier>\w+)_(?P<ts>\d{8}T\d{6})\.json$")


def _parse_recording_time(path: Path):
    """Ambil waktu asli pengambilan snapshot dari nama file rekaman --
    dipakai untuk mengurutkan replay dan menghitung jeda antar file."""
    m = _FILENAME_RE.match(path.name)
    if not m:
        return None
    return datetime.strptime(m.group("ts"), "%Y%m%dT%H%M%S")


def _write_atomic(content: bytes, dest_dir: Path, filename: str):
    """Sama seperti _write_ndjson_atomic di ingest.py -- tulis ke .tmp lalu
    rename, supaya streaming_job.py yang memantau folder ini tidak pernah
    membaca file yang sedang setengah ditulis."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_dir / f"{filename}.tmp"
    final_path = dest_dir / filename
    tmp_path.write_bytes(content)
    tmp_path.rename(final_path)


def replay(cfg, speed):
    """Baca semua rekaman terurut waktu, lalu tulis ulang ke landing_stream/
    dengan jeda proporsional terhadap waktu asli antar snapshot (dibagi
    `speed` supaya bisa dipercepat) -- mensimulasikan aliran data live tanpa
    memanggil OpenSky sama sekali."""
    recordings_dir = Path(cfg["paths"]["recordings"])
    landing_dir = Path(cfg["paths"]["landing_stream"])

    files = sorted(
        (p for p in recordings_dir.glob("states_*.json") if _parse_recording_time(p)),
        key=lambda p: _parse_recording_time(p),
    )
    if not files:
        print(f"Tidak ada rekaman di {recordings_dir}")
        return

    # run_id membedakan nama file replay dari file asli ingest.py yang
    # namanya identik -- supaya replay bisa dijalankan berkali-kali tanpa
    # saling menimpa file satu sama lain.
    run_id = uuid.uuid4().hex[:8]
    print(f"Replay {len(files)} file, speed={speed}x, run_id={run_id}")

    prev_time = None
    for path in files:
        rec_time = _parse_recording_time(path)
        if prev_time is not None:
            # Jeda antar file mengikuti selisih waktu ASLI saat direkam,
            # dibagi faktor speed -- speed=5 berarti jeda aslinya 60 detik
            # jadi cuma 12 detik saat diputar ulang.
            delay = (rec_time - prev_time).total_seconds() / speed
            if delay > 0:
                time.sleep(delay)
        prev_time = rec_time

        content = path.read_bytes()
        out_name = f"replay_{run_id}_{path.name}"
        _write_atomic(content, landing_dir, out_name)
        print(f"  -> {out_name} ({rec_time.isoformat()})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--speed", type=float, default=1.0, help="faktor percepatan (mis. 5 = 5x lebih cepat)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    replay(cfg, args.speed)


if __name__ == "__main__":
    main()

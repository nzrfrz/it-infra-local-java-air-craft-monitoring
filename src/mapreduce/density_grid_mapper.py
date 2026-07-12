"""Hadoop Streaming mapper untuk density_grid_hourly lewat MapReduce/YARN
asli (bukan Spark) -- lihat docs/design/2026-07-11-yarn-migration-design.md
untuk konteks kenapa Spark-on-YARN tidak dipakai di mesin ini.

Tiap baris stdin = isi 1 file JSON mentah OpenSky utuh (raw/dt=<tanggal>/*.json
ditulis single-line oleh ingest.py, lihat CONTRACTS.md C1), karena
TextInputFormat default membaca per baris dan file-nya memang cuma 1 baris.

Filter sama seperti batch_job.py's build_states_clean: bbox nasional +
rentang ketinggian. TIDAK melakukan dedup global (icao24, ts) seperti
batch_job.py -- itu butuh join lintas file yang di luar scope job
pembanding/demonstrasi YARN ini, jadi hasil aircraft_count di sini bisa
sedikit lebih tinggi dari density_grid_hourly versi Spark.
"""
import json
import math
import sys
from datetime import datetime, timezone

NATIONAL_BBOX = {"lamin": -11.0, "lamax": 6.0, "lomin": 95.0, "lomax": 141.0}
ALTITUDE_MIN_M, ALTITUDE_MAX_M = -100.0, 15000.0

# Index kolom array state OpenSky yang relevan untuk job ini saja (subset
# dari _IDX_* di common.py -- mapper ini sengaja berdiri sendiri tanpa
# import common.py, karena Hadoop Streaming mengirim script ini apa adanya
# ke setiap node cluster tanpa dependency lain).
_IDX_TIME_POSITION = 3
_IDX_LON = 5
_IDX_LAT = 6
_IDX_BARO_ALTITUDE = 7


def main():
    # Kontrak Hadoop Streaming: mapper membaca dari stdin, menulis pasangan
    # "key\tvalue" ke stdout -- Hadoop yang menangani pengiriman baris ke
    # proses ini dan pengumpulan/sorting outputnya ke reducer.
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            raw = json.loads(line)
        except ValueError:
            # Baris rusak/tidak valid JSON dilewati saja, bukan menghentikan
            # seluruh job -- satu file korup tidak boleh menggagalkan mapper lain.
            continue
        for s in raw.get("states") or []:
            ts = s[_IDX_TIME_POSITION]
            lat = s[_IDX_LAT]
            lon = s[_IDX_LON]
            if ts is None or lat is None or lon is None:
                continue
            if not (NATIONAL_BBOX["lamin"] <= lat <= NATIONAL_BBOX["lamax"]):
                continue
            if not (NATIONAL_BBOX["lomin"] <= lon <= NATIONAL_BBOX["lomax"]):
                continue
            alt = s[_IDX_BARO_ALTITUDE]
            if alt is not None and not (ALTITUDE_MIN_M <= alt <= ALTITUDE_MAX_M):
                continue
            grid_cell = f"{math.floor(lat)}_{math.floor(lon)}"
            hour = datetime.fromtimestamp(int(ts), tz=timezone.utc).hour
            # Key gabungan "grid_cell@hour" (bukan 2 kolom terpisah) --
            # Hadoop Streaming secara default hanya mengenali SATU key
            # (kolom sebelum tab pertama) untuk sorting/grouping ke reducer.
            print(f"{grid_cell}@{hour}\t1")


if __name__ == "__main__":
    main()

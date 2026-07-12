"""PySpark batch (harian): HDFS raw/dt=<tanggal> -> flatten -> cleaning/dedup
-> agregasi -> Parquet curated + daily_snapshot ke MongoDB.

Raw zone menyimpan respons asli OpenSky (dict {"time":.., "states":[[...]]});
di-flatten pakai fungsi yang SAMA dengan ingest.py (`common.flatten`, DRY)
lewat RDD map -- lebih andal daripada mengandalkan inferensi skema Spark atas
array JSON heterogen (string/number/bool campur dalam satu array).

Usage: python src/batch_job.py [--config config/config.yaml] [--date YYYY-MM-DD]
"""
import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Windows: perintah bare "python" bisa ke-intersep oleh Microsoft Store stub
# (tidak ada python.exe nyata di PATH bernama itu). Paksa worker Spark pakai
# python venv yang sama dengan driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    countDistinct,
    floor,
    from_unixtime,
    hour,
    lit,
    sqrt,
    substring,
    pow as spark_pow,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import AIRPORT_RADIUS_DEG, AIRPORTS, STATES_SCHEMA, flatten, load_config  # noqa: E402

# Nama file raw yang ditulis ingest.py: states_<tier>_<timestamp>.json --
# dipakai untuk mendeteksi tier (java/national) dari path filenya sendiri.
_FILENAME_RE = re.compile(r"^states_(?P<tier>\w+)_(?P<ts>\d{8}T\d{6})\.json$")

NATIONAL_BBOX = {"lamin": -11.0, "lamax": 6.0, "lomin": 95.0, "lomax": 141.0}
ALTITUDE_MIN_M, ALTITUDE_MAX_M = -100.0, 15000.0  # batas validasi wajar utk ketinggian


def _tier_from_path(path: str) -> str:
    """Ekstrak tier ("java"/"national") dari nama file, dipakai flatten()
    untuk mengisi kolom `tier` di setiap baris hasil batch."""
    m = _FILENAME_RE.match(Path(path).name)
    return m.group("tier") if m else "unknown"


def _hdfs_path_exists(spark, path: str) -> bool:
    """Cek keberadaan path HDFS lewat Hadoop FileSystem API Java (via py4j),
    dipakai sebelum job mulai membaca supaya bisa exit dengan pesan jelas
    kalau raw data untuk tanggal itu memang belum pernah di-ingest."""
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(hadoop_conf)
    return fs.exists(jvm_path)


def load_raw_as_rows(sc, raw_dir):
    """wholeTextFiles -> flatMap(common.flatten) -> list of dict rows.

    wholeTextFiles membaca tiap file JSON MENTAH sebagai satu (path, isi)
    utuh -- bukan per baris -- karena satu file = satu respons OpenSky
    lengkap yang perlu di-parse sebagai satu dokumen JSON."""
    files_rdd = sc.wholeTextFiles(raw_dir)

    def _flatten_file(pair):
        path, content = pair
        tier = _tier_from_path(path)
        raw = json.loads(content)
        fetched_at = raw.get("time") or 0
        # flatMap: 1 file raw -> banyak baris pesawat (pakai fungsi flatten
        # yang sama persis dipakai ingest.py, supaya hasilnya konsisten).
        return flatten(raw, tier, fetched_at)

    return files_rdd.flatMap(_flatten_file)


def build_states_clean(spark, rows_rdd, date_str):
    """Bangun DataFrame `states_clean`: baris mentah -> dedup -> validasi ->
    tambah kolom turunan (grid_cell, dt). Ini tabel dasar tempat semua
    agregasi lain (density, airport, summary) diturunkan."""
    df = spark.createDataFrame(rows_rdd, schema=STATES_SCHEMA)

    # Dedup: retry ingest atau overlap window polling bisa menghasilkan baris
    # (icao24, ts) yang identik lebih dari sekali -- ini lapisan cleaning
    # kedua (lapisan pertama ada di idempotensi nama file ingest.py).
    df = df.dropDuplicates(["icao24", "ts"])
    # Validasi 1: posisi harus di dalam bounding box Indonesia -- membuang
    # data nyasar/noise dari sumber (jarang, tapi bisa terjadi).
    df = df.filter(
        (col("lat") >= NATIONAL_BBOX["lamin"])
        & (col("lat") <= NATIONAL_BBOX["lamax"])
        & (col("lon") >= NATIONAL_BBOX["lomin"])
        & (col("lon") <= NATIONAL_BBOX["lomax"])
    )
    # Validasi 2: ketinggian masuk akal (atau null, yang tetap diterima --
    # bukan berarti datanya salah, cuma pesawat itu tidak melaporkan altitude).
    df = df.filter(
        col("baro_altitude_m").isNull()
        | ((col("baro_altitude_m") >= ALTITUDE_MIN_M) & (col("baro_altitude_m") <= ALTITUDE_MAX_M))
    )
    # grid_cell: sel 1x1 derajat (sama seperti common.zone_for, tapi versi
    # kolom Spark) -- dasar agregasi kepadatan per wilayah.
    df = df.withColumn("grid_cell", concat_ws("_", floor(col("lat")), floor(col("lon"))))
    df = df.withColumn("dt", lit(date_str))  # kolom partisi Hive-style saat ditulis ke Parquet
    return df


def aggregate_density_grid_hourly(states_clean, date_str):
    """Hitung jumlah pesawat per sel grid per jam -- dasar heatmap kepadatan
    di dashboard History (DensityHeatmap.tsx)."""
    return (
        states_clean.withColumn("hour", hour(from_unixtime(col("ts"))))
        .groupBy("grid_cell", "hour")
        .count()
        .withColumnRenamed("count", "aircraft_count")
        .withColumn("dt", lit(date_str))
    )


def aggregate_airport_hourly(states_clean, date_str):
    """Untuk tiap bandara utama, hitung pesawat per jam yang posisinya dalam
    radius AIRPORT_RADIUS_DEG darinya -- proxy sederhana untuk "traffic bandara"
    tanpa data resmi jadwal penerbangan."""
    result = None
    for code, (air_lat, air_lon) in AIRPORTS.items():
        # Jarak Euclidean sederhana dalam derajat (bukan haversine) --
        # cukup akurat untuk radius kecil (~0.5 derajat) di skala lokal ini.
        dist = sqrt(spark_pow(col("lat") - lit(air_lat), 2) + spark_pow(col("lon") - lit(air_lon), 2))
        near = (
            states_clean.filter(dist <= AIRPORT_RADIUS_DEG)
            .withColumn("airport", lit(code))
            .withColumn("hour", hour(from_unixtime(col("ts"))))
            .groupBy("airport", "hour")
            .count()
            .withColumnRenamed("count", "aircraft_count")
        )
        # unionByName: gabungkan hasil tiap bandara jadi satu DataFrame besar
        # (bukan 5 DataFrame terpisah).
        result = near if result is None else result.unionByName(near)
    return result.withColumn("dt", lit(date_str))


def aggregate_daily_summary(states_clean, density_grid_hourly, date_str, spark):
    """Ringkasan satu-baris-per-hari: total record, jumlah pesawat unik, jam
    tersibuk, dan top-10 prefix callsign (maskapai) -- ditampilkan sebagai
    kartu ringkasan di tab History dashboard."""
    total_records = states_clean.count()
    unique_aircraft = states_clean.select(countDistinct("icao24")).first()[0]

    # Jam dengan total aircraft_count tertinggi (dijumlahkan lintas semua grid_cell).
    busiest_row = (
        density_grid_hourly.groupBy("hour").sum("aircraft_count").orderBy(col("sum(aircraft_count)").desc()).first()
    )
    busiest_hour = busiest_row["hour"] if busiest_row else None

    # 3 huruf pertama callsign = kode maskapai (mis. "GIA123" -> "GIA" = Garuda Indonesia).
    top_airlines = (
        states_clean.filter(col("callsign").isNotNull())
        .withColumn("prefix", substring(col("callsign"), 1, 3))
        .groupBy("prefix")
        .count()
        .orderBy(col("count").desc())
        .limit(10)
        .collect()  # kecil (maks 10 baris) -> aman ditarik ke driver sebagai list Python
    )
    top_airlines_list = [{"prefix": r["prefix"], "count": r["count"]} for r in top_airlines]

    summary_row = spark.createDataFrame(
        [
            {
                "dt": date_str,
                "total_records": total_records,
                "unique_aircraft": unique_aircraft,
                "busiest_hour": busiest_hour,
            }
        ]
    )
    return summary_row, top_airlines_list, total_records, unique_aircraft, busiest_hour


def write_daily_snapshot_to_mongo(cfg, date_str, total_records, unique_aircraft, busiest_hour, top_airlines):
    """Tulis ringkasan harian ke MongoDB (koleksi daily_snapshot) -- ini yang
    dibaca endpoint REST /api/history/summary, bukan Parquet (lebih cepat
    untuk satu dokumen kecil per hari daripada scan Parquet tiap request)."""
    client = MongoClient(cfg["mongo"]["uri"])
    db = client[cfg["mongo"]["db"]]
    doc = {
        "_id": date_str,  # 1 dokumen per tanggal -> replace_one+upsert = idempotent, aman di-rerun
        "total_records": total_records,
        "unique_aircraft": unique_aircraft,
        "busiest_hour": busiest_hour,
        "top_airlines": top_airlines,
        "generated_at": datetime.now(timezone.utc),
    }
    db.daily_snapshot.replace_one({"_id": date_str}, doc, upsert=True)
    client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--date", default=None, help="YYYY-MM-DD, default kemarin (UTC)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    # Default "kemarin" karena batch job biasanya dijadwalkan jalan setelah
    # hari itu selesai (data sudah lengkap) -- tapi run_backend.ps1 memaksa
    # --date hari ini juga supaya History tab langsung ada data begitu backend start.
    date_str = args.date or (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    spark = SparkSession.builder.appName("opensky-batch").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # PENTING: mode "overwrite" + partitionBy("dt") default-nya "static" --
    # itu artinya SELURUH direktori output dihapus dulu sebelum menulis,
    # bukan cuma partisi dt=<tanggal> yang sedang diproses. Efeknya: tiap
    # kali batch_job.py jalan untuk satu tanggal, curated data tanggal LAIN
    # ikut terhapus diam-diam (ditemukan lewat History tab yang mendadak
    # 404 lagi untuk tanggal yang sebelumnya sudah berhasil). "dynamic" bikin
    # overwrite cuma menimpa partisi yang ada di DataFrame yang sedang
    # ditulis, partisi tanggal lain tidak disentuh.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Worker python (proses terpisah) tidak mewarisi sys.path driver -- kirim
    # common.py eksplisit supaya `flatten()` bisa di-import di dalam flatMap.
    spark.sparkContext.addPyFile(str(Path(__file__).resolve().parent / "common.py"))

    raw_dir = f"{cfg['hdfs']['base']}/raw/dt={date_str}"
    curated_base = f"{cfg['hdfs']['base']}/curated"

    if not _hdfs_path_exists(spark, raw_dir):
        # Exit code khusus (bukan exception generik / stack trace Spark yang
        # noise-nya beda-beda tiap run -- lihat api/main.py) supaya endpoint
        # /api/history/refresh bisa bedakan "memang belum ada data" dari
        # kegagalan job yang sesungguhnya secara deterministik.
        print(f"[batch_job] raw_dir tidak ada: {raw_dir} -- ingest.py belum pernah jalan untuk tanggal ini")
        spark.stop()
        sys.exit(2)

    # === Tahap 1: baca & bersihkan ===
    rows_rdd = load_raw_as_rows(spark.sparkContext, raw_dir)
    states_clean = build_states_clean(spark, rows_rdd, date_str).cache()  # cache: dipakai berkali-kali di bawah

    row_count = states_clean.count()
    print(f"[batch_job] dt={date_str} states_clean rows (setelah cleaning/dedup): {row_count}")
    if row_count == 0:
        print(f"[batch_job] tidak ada data untuk {date_str}, keluar tanpa menulis output")
        spark.stop()
        return

    # coalesce sebelum menulis -- volume harian di skala ini (ratusan/ribuan baris)
    # tidak butuh paralelisme default Spark (200 partisi -> 200 file kecil per
    # partisi dt, "small files problem" klasik Hadoop yang juga bikin lambat
    # dibaca balik lewat WebHDFS satu-per-satu di serving API)
    states_clean.coalesce(4).write.mode("overwrite").partitionBy("dt").parquet(f"{curated_base}/states_clean")

    # === Tahap 2: agregasi turunan, masing-masing ditulis sebagai dataset Parquet terpisah ===
    density_grid_hourly = aggregate_density_grid_hourly(states_clean, date_str)
    density_grid_hourly.coalesce(1).write.mode("overwrite").partitionBy("dt").parquet(
        f"{curated_base}/density_grid_hourly"
    )

    airport_hourly = aggregate_airport_hourly(states_clean, date_str)
    airport_hourly.coalesce(1).write.mode("overwrite").partitionBy("dt").parquet(f"{curated_base}/airport_hourly")

    summary_df, top_airlines, total_records, unique_aircraft, busiest_hour = aggregate_daily_summary(
        states_clean, density_grid_hourly, date_str, spark
    )
    summary_df.coalesce(1).write.mode("overwrite").partitionBy("dt").parquet(f"{curated_base}/daily_summary")

    # === Tahap 3: salinan ringkasan ke MongoDB (akses cepat utk REST API) ===
    write_daily_snapshot_to_mongo(cfg, date_str, total_records, unique_aircraft, busiest_hour, top_airlines)

    print(
        f"[batch_job] selesai: total_records={total_records} unique_aircraft={unique_aircraft} "
        f"busiest_hour={busiest_hour} top_airlines={top_airlines[:3]}"
    )
    spark.stop()


if __name__ == "__main__":
    main()

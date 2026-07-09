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

_FILENAME_RE = re.compile(r"^states_(?P<tier>\w+)_(?P<ts>\d{8}T\d{6})\.json$")

NATIONAL_BBOX = {"lamin": -11.0, "lamax": 6.0, "lomin": 95.0, "lomax": 141.0}
ALTITUDE_MIN_M, ALTITUDE_MAX_M = -100.0, 15000.0


def _tier_from_path(path: str) -> str:
    m = _FILENAME_RE.match(Path(path).name)
    return m.group("tier") if m else "unknown"


def load_raw_as_rows(sc, raw_dir):
    """wholeTextFiles -> flatMap(common.flatten) -> list of dict rows."""
    files_rdd = sc.wholeTextFiles(raw_dir)

    def _flatten_file(pair):
        path, content = pair
        tier = _tier_from_path(path)
        raw = json.loads(content)
        fetched_at = raw.get("time") or 0
        return flatten(raw, tier, fetched_at)

    return files_rdd.flatMap(_flatten_file)


def build_states_clean(spark, rows_rdd, date_str):
    df = spark.createDataFrame(rows_rdd, schema=STATES_SCHEMA)

    df = df.dropDuplicates(["icao24", "ts"])
    df = df.filter(
        (col("lat") >= NATIONAL_BBOX["lamin"])
        & (col("lat") <= NATIONAL_BBOX["lamax"])
        & (col("lon") >= NATIONAL_BBOX["lomin"])
        & (col("lon") <= NATIONAL_BBOX["lomax"])
    )
    df = df.filter(
        col("baro_altitude_m").isNull()
        | ((col("baro_altitude_m") >= ALTITUDE_MIN_M) & (col("baro_altitude_m") <= ALTITUDE_MAX_M))
    )
    df = df.withColumn("grid_cell", concat_ws("_", floor(col("lat")), floor(col("lon"))))
    df = df.withColumn("dt", lit(date_str))
    return df


def aggregate_density_grid_hourly(states_clean, date_str):
    return (
        states_clean.withColumn("hour", hour(from_unixtime(col("ts"))))
        .groupBy("grid_cell", "hour")
        .count()
        .withColumnRenamed("count", "aircraft_count")
        .withColumn("dt", lit(date_str))
    )


def aggregate_airport_hourly(states_clean, date_str):
    result = None
    for code, (air_lat, air_lon) in AIRPORTS.items():
        dist = sqrt(spark_pow(col("lat") - lit(air_lat), 2) + spark_pow(col("lon") - lit(air_lon), 2))
        near = (
            states_clean.filter(dist <= AIRPORT_RADIUS_DEG)
            .withColumn("airport", lit(code))
            .withColumn("hour", hour(from_unixtime(col("ts"))))
            .groupBy("airport", "hour")
            .count()
            .withColumnRenamed("count", "aircraft_count")
        )
        result = near if result is None else result.unionByName(near)
    return result.withColumn("dt", lit(date_str))


def aggregate_daily_summary(states_clean, density_grid_hourly, date_str, spark):
    total_records = states_clean.count()
    unique_aircraft = states_clean.select(countDistinct("icao24")).first()[0]

    busiest_row = (
        density_grid_hourly.groupBy("hour").sum("aircraft_count").orderBy(col("sum(aircraft_count)").desc()).first()
    )
    busiest_hour = busiest_row["hour"] if busiest_row else None

    top_airlines = (
        states_clean.filter(col("callsign").isNotNull())
        .withColumn("prefix", substring(col("callsign"), 1, 3))
        .groupBy("prefix")
        .count()
        .orderBy(col("count").desc())
        .limit(10)
        .collect()
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
    client = MongoClient(cfg["mongo"]["uri"])
    db = client[cfg["mongo"]["db"]]
    doc = {
        "_id": date_str,
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
    date_str = args.date or (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    spark = SparkSession.builder.appName("opensky-batch").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Worker python (proses terpisah) tidak mewarisi sys.path driver -- kirim
    # common.py eksplisit supaya `flatten()` bisa di-import di dalam flatMap.
    spark.sparkContext.addPyFile(str(Path(__file__).resolve().parent / "common.py"))

    raw_dir = f"{cfg['hdfs']['base']}/raw/dt={date_str}"
    curated_base = f"{cfg['hdfs']['base']}/curated"

    rows_rdd = load_raw_as_rows(spark.sparkContext, raw_dir)
    states_clean = build_states_clean(spark, rows_rdd, date_str).cache()

    row_count = states_clean.count()
    print(f"[batch_job] dt={date_str} states_clean rows (setelah cleaning/dedup): {row_count}")
    if row_count == 0:
        print(f"[batch_job] tidak ada data untuk {date_str}, keluar tanpa menulis output")
        spark.stop()
        return

    states_clean.write.mode("overwrite").partitionBy("dt").parquet(f"{curated_base}/states_clean")

    density_grid_hourly = aggregate_density_grid_hourly(states_clean, date_str)
    density_grid_hourly.write.mode("overwrite").partitionBy("dt").parquet(f"{curated_base}/density_grid_hourly")

    airport_hourly = aggregate_airport_hourly(states_clean, date_str)
    airport_hourly.write.mode("overwrite").partitionBy("dt").parquet(f"{curated_base}/airport_hourly")

    summary_df, top_airlines, total_records, unique_aircraft, busiest_hour = aggregate_daily_summary(
        states_clean, density_grid_hourly, date_str, spark
    )
    summary_df.write.mode("overwrite").partitionBy("dt").parquet(f"{curated_base}/daily_summary")

    write_daily_snapshot_to_mongo(cfg, date_str, total_records, unique_aircraft, busiest_hour, top_airlines)

    print(
        f"[batch_job] selesai: total_records={total_records} unique_aircraft={unique_aircraft} "
        f"busiest_hour={busiest_hour} top_airlines={top_airlines[:3]}"
    )
    spark.stop()


if __name__ == "__main__":
    main()

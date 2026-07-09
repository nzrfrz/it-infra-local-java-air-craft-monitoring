"""Spark Structured Streaming: landing_stream/ (NDJSON) -> agregat & alert -> MongoDB.

3 query independen (checkpoint terpisah):
  1. live_states  : upsert posisi terkini per pesawat (icao24)
  2. zone_stats   : window 2 menit per zona (count, avg velocity) + deteksi density_spike
  3. alerts       : squawk darurat (7500/7600/7700)

Tulis via pymongo langsung di foreachBatch (bukan mongo-spark-connector) --
volume prototipe kecil, driver.collect() per micro-batch cukup & menghindari
kerumitan resolusi jar connector di Windows.

Usage: python src/streaming_job.py [--config config/config.yaml]
"""
import argparse
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, ReplaceOne
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, to_timestamp, window, count, avg, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import load_config  # noqa: E402

EMERGENCY_SQUAWKS = ("7500", "7600", "7700")
DENSITY_SPIKE_FACTOR = 3.0

SCHEMA = StructType(
    [
        StructField("icao24", StringType(), False),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("ts", LongType(), False),
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False),
        StructField("baro_altitude_m", DoubleType(), True),
        StructField("velocity_ms", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("squawk", StringType(), True),
        StructField("tier", StringType(), False),
        StructField("fetched_at", LongType(), True),
    ]
)


def _as_file_uri(path: str) -> str:
    """Path lokal Windows (mis. 'D:/bigdata/landing_stream') perlu skema
    eksplisit 'file:///' karena fs.defaultFS di cluster ini adalah HDFS."""
    if "://" in path:
        return path
    return "file:///" + path.lstrip("/")


def build_base_stream(spark, cfg):
    df = (
        spark.readStream.format("json")
        .schema(SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .load(_as_file_uri(cfg["paths"]["landing_stream"]))
    )
    return df.withColumn("zone", concat_ws("_", floor(col("lat")), floor(col("lon")))).withColumn(
        "event_time", to_timestamp(col("ts"))
    )


def make_mongo_client(cfg):
    return MongoClient(cfg["mongo"]["uri"])


def upsert_live_states(cfg):
    def _fn(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
        client = make_mongo_client(cfg)
        db = client[cfg["mongo"]["db"]]
        ops = []
        for r in rows:
            doc = {
                "_id": r["icao24"],
                "callsign": r["callsign"],
                "origin_country": r["origin_country"],
                "lat": r["lat"],
                "lon": r["lon"],
                "baro_altitude_m": r["baro_altitude_m"],
                "velocity_ms": r["velocity_ms"],
                "true_track": r["true_track"],
                "on_ground": r["on_ground"],
                "squawk": r["squawk"],
                "ts": r["ts"],
                "updated_at": datetime.now(timezone.utc),
            }
            ops.append(ReplaceOne({"_id": r["icao24"]}, doc, upsert=True))
        if ops:
            db.live_states.bulk_write(ops)
        client.close()

    return _fn


def insert_emergency_alerts(cfg):
    def _fn(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
        client = make_mongo_client(cfg)
        db = client[cfg["mongo"]["db"]]
        docs = [
            {
                "type": "emergency_squawk",
                "icao24": r["icao24"],
                "zone": None,
                "ts": r["ts"],
                "severity": "critical",
                "details": f"squawk {r['squawk']}",
                "created_at": datetime.now(timezone.utc),
            }
            for r in rows
        ]
        if docs:
            db.alerts.insert_many(docs)
        client.close()

    return _fn


def upsert_zone_stats_and_detect_spike(cfg):
    def _fn(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
        client = make_mongo_client(cfg)
        db = client[cfg["mongo"]["db"]]

        zone_ops = []
        alert_docs = []
        one_hour_ago_epoch = None

        for r in rows:
            window_start = int(r["window"]["start"].timestamp())
            window_end = int(r["window"]["end"].timestamp())
            zone = r["zone"]
            aircraft_count = r["aircraft_count"]
            avg_velocity = r["avg_velocity_ms"]

            zone_ops.append(
                ReplaceOne(
                    {"_id": f"{zone}_{window_start}"},
                    {
                        "_id": f"{zone}_{window_start}",
                        "zone": zone,
                        "window_start": window_start,
                        "window_end": window_end,
                        "aircraft_count": aircraft_count,
                        "avg_velocity_ms": avg_velocity,
                        "updated_at": datetime.now(timezone.utc),
                    },
                    upsert=True,
                )
            )

            if one_hour_ago_epoch is None:
                one_hour_ago_epoch = window_start - 3600
            baseline_docs = list(
                db.zone_stats.find(
                    {"zone": zone, "window_start": {"$gte": one_hour_ago_epoch, "$lt": window_start}}
                )
            )
            if baseline_docs:
                baseline_avg = sum(d["aircraft_count"] for d in baseline_docs) / len(baseline_docs)
                if baseline_avg > 0 and aircraft_count > DENSITY_SPIKE_FACTOR * baseline_avg:
                    alert_docs.append(
                        {
                            "type": "density_spike",
                            "icao24": None,
                            "zone": zone,
                            "ts": window_start,
                            "severity": "warning",
                            "details": f"kepadatan {aircraft_count} > {DENSITY_SPIKE_FACTOR}x baseline ({baseline_avg:.1f})",
                            "created_at": datetime.now(timezone.utc),
                        }
                    )

        if zone_ops:
            db.zone_stats.bulk_write(zone_ops)
        if alert_docs:
            db.alerts.insert_many(alert_docs)
        client.close()

    return _fn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    spark = SparkSession.builder.appName("opensky-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    base = build_base_stream(spark, cfg)
    checkpoint_base = cfg["paths"]["checkpoint"]

    q1 = (
        base.writeStream.foreachBatch(upsert_live_states(cfg))
        .option("checkpointLocation", f"{checkpoint_base}/q1_live_states")
        .trigger(processingTime="10 seconds")
        .start()
    )

    q3 = (
        base.filter(col("squawk").isin(list(EMERGENCY_SQUAWKS)))
        .writeStream.foreachBatch(insert_emergency_alerts(cfg))
        .option("checkpointLocation", f"{checkpoint_base}/q3_alerts")
        .trigger(processingTime="10 seconds")
        .start()
    )

    zone_agg = (
        base.withWatermark("event_time", "1 minute")
        .dropDuplicates(["icao24", "ts"])  # baris duplikat (retry ingest, dsb) tidak menggandakan count
        .groupBy(window(col("event_time"), "2 minutes"), col("zone"))
        .agg(count("*").alias("aircraft_count"), avg("velocity_ms").alias("avg_velocity_ms"))
    )
    q2 = (
        zone_agg.writeStream.outputMode("update")
        .foreachBatch(upsert_zone_stats_and_detect_spike(cfg))
        .option("checkpointLocation", f"{checkpoint_base}/q2_zone_stats")
        .trigger(processingTime="10 seconds")
        .start()
    )

    for q in (q1, q2, q3):
        q.awaitTermination()


if __name__ == "__main__":
    main()

"""Spark Structured Streaming: Kafka topic opensky.states -> agregat & alert -> MongoDB.

Sumber data sebelumnya folder lokal landing_stream/ (file JSON), sekarang Kafka
(lihat docs/design/2026-07-12-kafka-migration-design.md) -- payload JSON per
pesan tidak berubah, cuma medium transportnya.

3 query independen (checkpoint terpisah):
  1. live_states  : upsert posisi terkini per pesawat (icao24)
  2. zone_stats   : window 2 menit per zona (count, avg velocity) + deteksi density_spike
  3. alerts       : squawk darurat (7500/7600/7700)

Tulis via pymongo langsung di foreachBatch (bukan mongo-spark-connector) --
volume prototipe kecil, driver.collect() per micro-batch cukup & menghindari
kerumitan resolusi jar connector di Windows.

Usage: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/streaming_job.py [--config config/config.yaml]
"""
import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Windows: perintah bare "python" bisa ke-intersep oleh Microsoft Store stub.
# Paksa worker Spark pakai python venv yang sama dengan driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pymongo import MongoClient, ReplaceOne
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, to_timestamp, window, count, avg, concat_ws, from_json

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import load_config, STATES_SCHEMA  # noqa: E402

EMERGENCY_SQUAWKS = ("7500", "7600", "7700")  # kode transponder darurat standar penerbangan internasional
DENSITY_SPIKE_FACTOR = 3.0  # anomali = kepadatan zona > 3x rata-rata baseline 1 jam terakhir


def build_base_stream(spark, cfg):
    """Bangun streaming DataFrame dasar yang dipakai ketiga query (q1/q2/q3):
    baca pesan baru dari Kafka topic opensky.states (ditulis ingest.py) sebagai
    stream, parse value (JSON) sesuai STATES_SCHEMA, lalu tambah kolom turunan
    `zone` (grid 1 derajat) dan `event_time` (untuk watermark)."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("subscribe", cfg["kafka"]["topic"])
        .option("startingOffsets", "latest")  # jalur live, bukan replay historis -- mulai dari pesan terbaru
        .option("maxOffsetsPerTrigger", 2000)  # batasi pesan per micro-batch, jaga latensi tetap rendah & stabil
        .load()
    )
    df = raw.select(
        from_json(col("value").cast("string"), STATES_SCHEMA).alias("data")
    ).select("data.*")
    return df.withColumn("zone", concat_ws("_", floor(col("lat")), floor(col("lon")))).withColumn(
        "event_time", to_timestamp(col("ts"))
    )


def make_mongo_client(cfg):
    # Dibuat baru per pemanggilan foreachBatch (bukan dibagi/global) --
    # foreachBatch bisa dieksekusi di banyak partisi/executor berbeda,
    # dan MongoClient tidak aman dipakai lintas proses tanpa penanganan khusus.
    return MongoClient(cfg["mongo"]["uri"])


def upsert_live_states(cfg):
    """Query 1: untuk tiap micro-batch, upsert posisi terkini tiap pesawat ke
    koleksi `live_states` (kunci = icao24) -- ini yang dibaca WebSocket
    /ws/live lewat MongoDB change stream untuk menggerakkan peta di frontend."""
    def _fn(batch_df, batch_id):
        rows = batch_df.collect()  # micro-batch kecil (maks 10 file), aman ditarik ke driver
        if not rows:
            return
        client = make_mongo_client(cfg)
        db = client[cfg["mongo"]["db"]]
        ops = []
        for r in rows:
            doc = {
                "_id": r["icao24"],  # upsert by icao24 -> tidak mungkin duplikat per pesawat (exactly-once)
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
            db.live_states.bulk_write(ops)  # 1 round-trip untuk seluruh batch, bukan per-dokumen
        client.close()

    return _fn


def insert_emergency_alerts(cfg):
    """Query 3: baris dengan squawk darurat (di-filter SEBELUM sampai sini,
    lihat main()) langsung dicatat sebagai alert baru -- setiap kemunculan
    dicatat (insert, bukan upsert), karena tiap kejadian relevan untuk log."""
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
    """Query 2: untuk tiap window 2 menit per zona, simpan statistik
    (jumlah pesawat, rata-rata kecepatan) ke `zone_stats`, DAN bandingkan
    dengan baseline 1 jam terakhir untuk zona yang sama -- kalau melonjak
    > DENSITY_SPIKE_FACTOR kali lipat, catat sebagai alert anomali."""
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

            # Kunci dokumen = "{zona}_{window_start}" -> upsert idempoten,
            # window yang sama diproses ulang (mis. late data) tidak menduplikasi baris.
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

            # Ambil histori zona yang sama dalam 1 jam terakhir (sebelum window
            # ini) dari MongoDB sendiri sebagai baseline pembanding -- deteksi
            # anomali "on the fly" tanpa perlu state store Spark terpisah.
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
    # Suffix "_kafka" baru -- checkpoint lama (file source landing_stream/)
    # menyimpan offset dalam format yang tidak kompatibel dengan Kafka source
    # (mekanisme tracking beda total). Path baru = mulai fresh, tidak perlu
    # hapus manual checkpoint lama (checkpoint lama dibiarkan begitu saja,
    # tidak pernah dibaca lagi setelah ini).
    checkpoint_base = cfg["paths"]["checkpoint"] + "_kafka"

    # --- Query 1: live_states (posisi terkini per pesawat) ---
    # checkpointLocation TERPISAH per query -- masing-masing query punya
    # progress/offset tracking sendiri, jadi kalau salah satu di-restart,
    # yang lain tidak ikut mengulang dari awal.
    q1 = (
        base.writeStream.foreachBatch(upsert_live_states(cfg))
        .option("checkpointLocation", f"{checkpoint_base}/q1_live_states")
        .trigger(processingTime="10 seconds")  # micro-batch tiap 10 detik
        .start()
    )

    # --- Query 3: alerts darurat (squawk 7500/7600/7700) ---
    # Filter squawk darurat dilakukan di level Spark SEBELUM foreachBatch,
    # supaya micro-batch yang dikirim ke MongoDB memang hanya baris relevan.
    q3 = (
        base.filter(col("squawk").isin(list(EMERGENCY_SQUAWKS)))
        .writeStream.foreachBatch(insert_emergency_alerts(cfg))
        .option("checkpointLocation", f"{checkpoint_base}/q3_alerts")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- Query 2: zone_stats (agregasi berjendela 2 menit per zona) ---
    zone_agg = (
        base.withWatermark("event_time", "1 minute")  # toleransi data telat 1 menit sebelum window ditutup
        .dropDuplicates(["icao24", "ts"])  # baris duplikat (retry ingest, dsb) tidak menggandakan count
        .groupBy(window(col("event_time"), "2 minutes"), col("zone"))
        .agg(count("*").alias("aircraft_count"), avg("velocity_ms").alias("avg_velocity_ms"))
    )
    q2 = (
        zone_agg.writeStream.outputMode("update")  # "update": kirim baris yang berubah saja, bukan snapshot penuh tiap trigger
        .foreachBatch(upsert_zone_stats_and_detect_spike(cfg))
        .option("checkpointLocation", f"{checkpoint_base}/q2_zone_stats")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Blokir proses utama sampai salah satu query berhenti/gagal -- ketiganya
    # jalan konkuren di thread Spark internal masing-masing.
    for q in (q1, q2, q3):
        q.awaitTermination()


if __name__ == "__main__":
    main()

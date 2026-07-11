# Monitoring Lalu Lintas Udara Indonesia — Big Data Pipeline

Pipeline end-to-end (pola Lambda) untuk monitoring lalu lintas udara Indonesia secara real-time, dibangun dari data publik **OpenSky Network**. Tugas mata kuliah: desain infrastruktur Big Data untuk permasalahan interdisipliner (logistik, smart city, lingkungan).

Dokumen desain arsitektur lengkap: [`docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md`](docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md)
Kontrak data antar komponen: [`docs/design/CONTRACTS.md`](docs/design/CONTRACTS.md)
Rencana implementasi: [`docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md`](docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md)

## Arsitektur singkat

```
OpenSky API ─► ingest.py ─┬─► HDFS raw zone (arsip)
                           └─► landing_stream/ (NDJSON)
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                              ▼
          streaming_job.py                  batch_job.py
    (Spark Structured Streaming)             (PySpark, harian)
                    │                              │
                    ▼                              ▼
              MongoDB (live_states,          HDFS curated (Parquet)
              zone_stats, alerts)             + daily_snapshot → Mongo
                    │                              │
                    └──────────► api/ (FastAPI) ◄──┘
                       WebSocket /ws/live · REST /api/history/*
                                   │
                                   ▼
                        web/ (React + deck.gl/MapLibre)
```

## Struktur repo

```
src/            # ingest.py, replay.py, batch_job.py, streaming_job.py, common.py
api/            # FastAPI serving layer
web/            # React SPA (Vite + deck.gl/MapLibre)
config/         # config.yaml (lokal, tidak di-commit) + config.example.yaml
scripts/        # helper start/stop infra, eksperimen chaos, demo
fixtures/       # data contoh untuk dev tanpa perlu infra penuh jalan
docs/           # dokumen desain, kontrak, rencana implementasi, laporan
```

## Setup

1. Aktifkan venv bersama di root `#bigdata` (dipakai lintas project big data di mesin ini): `D:\Coding\#bigdata\venv\Scripts\activate` lalu `pip install -r requirements.txt`. Tidak ada `venv/` lokal di folder `it-infra`.
2. Salin `config/config.example.yaml` → `config/config.yaml`, isi kredensial OpenSky (`opensky-network.org` → API client)
3. Pastikan Hadoop/HDFS & MongoDB (replica set 1 node) sudah jalan — lihat `scripts/start_infra.ps1`
4. Jalankan komponen sesuai kebutuhan (lihat masing-masing skrip di `src/`, `api/`, `web/`)

## Status implementasi

Lihat checklist di [`docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md`](docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md).

# Indonesia Air Traffic Monitoring — Big Data Pipeline

*Baca dalam [Bahasa Indonesia](README-id.md).*

End-to-end pipeline (Lambda architecture) for real-time monitoring of Indonesian air traffic, built on public **OpenSky Network** data. Coursework project: designing Big Data infrastructure for an interdisciplinary problem (logistics, smart city, environment).

Full architecture design document: [`docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md`](docs/design/2026-07-08-desain-infrastruktur-bigdata-lalu-lintas-udara.md)
Data contracts between components: [`docs/design/CONTRACTS.md`](docs/design/CONTRACTS.md)
Implementation plan: [`docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md`](docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md)

> The docs above are still in Indonesian (original design artifacts for the coursework). This README is the English entry point for running the project.

## Architecture at a glance

```
OpenSky API ─► ingest.py ─┬─► HDFS raw zone (archive)
                           └─► Kafka topic opensky.states (KRaft, key=icao24)
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                              ▼
          streaming_job.py                  batch_job.py
    (Spark Structured Streaming)             (PySpark, daily)
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

> The transport layer used to be a local folder `landing_stream/` (NDJSON), migrated to Kafka
> on 2026-07-12 — see [`docs/design/2026-07-12-kafka-migration-design.md`](docs/design/2026-07-12-kafka-migration-design.md).

## Repo structure

```
src/            # ingest.py, replay.py, batch_job.py, streaming_job.py, common.py
api/            # FastAPI serving layer
web/            # React SPA (Vite + deck.gl/MapLibre)
config/         # config.yaml (local, not committed) + config.example.yaml
scripts/        # infra start/stop helpers, chaos experiments, demo
fixtures/       # sample data for dev without the full infra running
docs/           # design docs, contracts, implementation plans, reports
```

## Prerequisites

Infrastructure is installed manually (no package manager/container), tested with these versions running together:

| Component | Tested version | Notes |
|---|---|---|
| Java (JDK) | 8 (1.8.0_471) | Hadoop 3.3.x requires JDK 8 or 11 — JDK 17+ is not fully supported |
| Hadoop | 3.3.6 | HDFS + YARN, single-node (pseudo-distributed) |
| Spark | 3.5.1 (bin-hadoop3) | `spark-submit` runs `streaming_job.py` & `batch_job.py` |
| Kafka | 3.9.2 (Scala 2.13) | KRaft mode (no ZooKeeper) |
| MongoDB | 8.0, 1-node replica set (`rs0`) | Change streams used by `api/main.py` for `/ws/live` |
| Python | 3.11 | See `requirements.txt` |
| Node.js | 22.x | For `web/` (frontend, separate `it-infra-web` repo) |

None of the tools above need to live at the same path as the original developer's machine — see the next section.

## Local path configuration (so it runs on anyone's machine)

This repo does not hardcode where Hadoop/Spark/Kafka/the venv are installed on any given machine. What you need to set up locally:

1. **`JAVA_HOME`, `HADOOP_HOME`, `SPARK_HOME`, `KAFKA_HOME`** — standard Windows System Environment Variables, pointed at wherever you installed each tool (see the Prerequisites table for versions). Every script (`scripts/*.ps1`) and manual command in this README reads these variables, never a literal path — so install them on whatever drive/folder you like.
2. **Python venv** — two options:
   - **Just create a venv inside this repo** (simplest for new contributors):
     ```
     python -m venv venv
     venv\Scripts\activate
     pip install -r requirements.txt
     ```
     Every script in `scripts/` auto-detects `venv\Scripts\python.exe` at the repo root as the default when there's no override.
   - **Use one shared venv elsewhere** (e.g. shared across several big data projects) — set the env var `BIGDATA_VENV_PYTHON` to that venv's `python.exe`, or just `activate` it before running scripts (scripts also detect `VIRTUAL_ENV`). Resolution order in every script: `BIGDATA_VENV_PYTHON` → active venv (`VIRTUAL_ENV`) → fallback `venv\Scripts\python.exe` at the repo root.
3. **`config/config.yaml`** — copy from `config/config.example.yaml`, fill in your own OpenSky credentials (see §Setup).

## Setup

1. Set up a venv (see §Local path configuration above), then `pip install -r requirements.txt`.
2. Copy `config/config.example.yaml` → `config/config.yaml`, fill in OpenSky credentials (sign up at `opensky-network.org` → create an OAuth2 API client).
3. Make sure Hadoop/HDFS, YARN, Kafka, & MongoDB (1-node replica set) are installed and `JAVA_HOME`/`HADOOP_HOME`/`SPARK_HOME`/`KAFKA_HOME` are set — see §Prerequisites & §Local path configuration.
4. Run the components you need — see §Automatic run or §Manual run below.

## Automatic run

Fastest way to bring up every component at once:

```
scripts\run_backend.cmd
```

(or `scripts\run_backend.ps1` directly from PowerShell). This script will:

1. Start HDFS + check MongoDB (`start_infra.ps1`)
2. Start the Kafka broker
3. Start the API (`uvicorn`, port 8000)
4. Start `ingest.py` (OpenSky poller)
5. Start `streaming_job.py` (Spark Structured Streaming)
6. Run `batch_job.py` once for today's date (so the History tab has data immediately)

Every long-running component opens in its own window so you can watch its logs directly. Prerequisites before using this script:

- A venv already exists (see §Local path configuration) — the script fails with a clear error message if it can't find one.
- `KAFKA_HOME` is set and the broker **has been formatted once** (`kafka-storage.bat format` — see [`docs/plans/2026-07-12-kafka-migration.md`](docs/plans/2026-07-12-kafka-migration.md) Task 1, one-time per install).
- The HDFS namenode has been formatted once, and the MongoDB replica set `rs0` has been through `rs.initiate()` once — one-time per install, see §Manual run steps 1–2 for details if you haven't set this up yet.

**Not** started automatically (run manually if needed):

- `src\replay.py` — offline alternative to `ingest.py` using recorded data (not meant to run alongside `ingest.py`)
- `web/` (frontend) — `npm run dev` in the separate `it-infra-web` repo

Other helper scripts in `scripts/`:

| Script | Purpose |
|---|---|
| `start_infra.ps1` | Just starts HDFS + checks MongoDB (called by `run_backend.ps1`, can also be run standalone) |
| `run_batch_daily.ps1 -Date YYYY-MM-DD` | Runs `batch_job.py` for a given date (defaults to yesterday); can be registered with Task Scheduler |
| `run_yarn_density_grid.ps1 -Date YYYY-MM-DD` | MapReduce job (Hadoop Streaming), an alternative to Spark-on-YARN — see comments in the file for context |
| `demo.ps1` | Guides an end-to-end presentation/demo (~15 min), does not start components itself |

## Manual run (step-by-step)

Precondition: HDFS (`start-dfs.cmd`), YARN (`start-yarn.cmd`), the MongoDB replica set `rs0`, and Kafka (KRaft storage formatted once — see [`docs/plans/2026-07-12-kafka-migration.md`](docs/plans/2026-07-12-kafka-migration.md) Task 1) have already been set up. The steps below **turn on** services/components that have already been set up once, not a from-scratch setup.

The example commands below assume a venv at the repo root (`venv\Scripts\...`) — swap in your own venv's location if you're using the `BIGDATA_VENV_PYTHON`/separate-venv option (see §Local path configuration).

### 1. HDFS + YARN

```
start-dfs.cmd
start-yarn.cmd
```

Check all processes are alive:

```
jps
```

You should see `NameNode`, `DataNode`, `ResourceManager`, `NodeManager` (other JVM processes like `AppKt` are unrelated to this project — `jps` lists every JVM process on the machine).

### 2. MongoDB — verify the replica set is up

MongoDB isn't a JVM process, so it won't show up in `jps`. Check with `mongosh`:

```
mongosh "mongodb://localhost:27017/?replicaSet=rs0"
```

Inside the shell:

```js
use opensky
db.live_states.countDocuments()
db.zone_stats.countDocuments()
db.alerts.countDocuments()
```

To watch upserts arrive **in real time** (handy while testing `streaming_job.py` in step 6) — use a change stream, `cursor.next()` blocks until a new event arrives (not `.on('change', ...)`, that's the Node.js driver API, not supported in `mongosh`):

```js
const cs = db.watch([], {fullDocument: "updateLookup"})
while (true) { printjson(cs.next()) }
```

Leave this window open separately during development — upsert events will print automatically as they land.

### 3. Kafka broker

```
kafka-server-start.bat %KAFKA_HOME%\config\kraft\server.properties
```

(Requires `KAFKA_HOME` and `%KAFKA_HOME%\bin\windows` on PATH — see §Local path configuration. The broker must already have been formatted once, `kafka-storage.bat format`, no need to repeat on every start.)

Check the `opensky.states` topic exists:

```
kafka-topics.bat --list --bootstrap-server localhost:9092
```

Watch messages arrive in real time (optional, similar to the `mongosh watch()` above but at the Kafka level):

```
kafka-console-consumer.bat --topic opensky.states --bootstrap-server localhost:9092
```

### 4. API (FastAPI/uvicorn)

```
cd <your-repo-path>
venv\Scripts\activate
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

**Don't** add `--reload` — on Windows that makes `/api/history/refresh` always fail (the `--reload` worker runs under `SelectorEventLoop`, not `ProactorEventLoop`, which can't spawn subprocesses at all). Restart this window manually when you edit `api/main.py`.

### 5. ingest.py (OpenSky poller)

New terminal window — **must** be run from the repo root (the `src\...` paths below are relative to it):

```
cd <your-repo-path>
venv\Scripts\activate
python src\ingest.py
```

Publishes raw JSON to HDFS `raw/dt=<date>/` (archive) **and** to the Kafka topic `opensky.states` (consumed in step 6). Without `streaming_job.py` running, data piles up in Kafka but MongoDB stays empty — that's expected, not a bug.

### 6. streaming_job.py (Spark Structured Streaming)

New terminal window — **must** be run from the repo root, since the `src\streaming_job.py` path below is relative to it. You also **must** set `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` **before** calling `spark-submit` (setting them from inside the `.py` file is too late — the Spark driver process has already started with a plain `python` from PATH by then):

```
cd <your-repo-path>
set PYSPARK_PYTHON=<your-repo-path>\venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=<your-repo-path>\venv\Scripts\python.exe
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src\streaming_job.py
```

`--packages` is required (the Kafka connector isn't part of default Spark) — it's downloaded via Ivy once (needs internet), then cached locally for later runs. After a moment, check `http://localhost:4040` — **the Spark UI only appears once `spark-submit` is actually running** (HDFS/YARN infra alone won't bring up port 4040, that's expected).

### 7. batch_job.py (optional, manual per date)

Same as step 6 — run from the repo root:

```
cd <your-repo-path>
spark-submit src\batch_job.py --date 2026-07-12
```

(The `PYSPARK_*` env vars from step 6 still apply if run in the same window; set them again as in step 6 if you're in a new window.)

### 8. Frontend (optional)

```
cd <your-it-infra-web-repo-path>
npm run dev
```

## Implementation status

See the checklist in [`docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md`](docs/plans/2026-07-08-rencana-implementasi-opensky-pipeline.md).

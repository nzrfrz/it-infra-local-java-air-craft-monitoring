# YARN Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Submit `streaming_job.py` and `batch_job.py` to YARN
(`--master yarn --deploy-mode client`) instead of falling back to Spark's
default `local[*]`, so the project actually uses the resource-manager
separation its own design doc documents as a scale limitation.

**Design doc:** `it-infra/docs/design/2026-07-11-yarn-migration-design.md`
— read it before starting, this plan assumes its decisions (client mode,
1 executor/app, `yarn.nodemanager.resource.memory-mb=4096`) without
re-justifying them here.

**Architecture:** No new files. Three existing spark-submit call sites
(`scripts/run_batch_daily.ps1`, `scripts/run_backend.ps1` ×2, `api/main.py`'s
`history_refresh()`) get `HADOOP_CONF_DIR`/`YARN_CONF_DIR` env vars plus
`--master yarn --deploy-mode client` and sizing `--conf` flags added. One
Hadoop config file (`yarn-site.xml`, outside both git repos) gets a memory
cap lowered. `streaming_job.py`/`batch_job.py` themselves need **no source
changes** — YARN master/deploy-mode/sizing are all `spark-submit` CLI
concerns, not application code.

**Tech Stack:** Spark 3.5.1 / Hadoop 3.3.6 (already installed), PowerShell,
Python (FastAPI). No new dependencies.

## Global Constraints

- **No contract changes.** `docs/design/CONTRACTS.md` (C1–C4) documents
  data/API contracts — this migration changes *how* Spark jobs execute, not
  *what* they produce or any endpoint shape. Confirmed while writing this
  plan: nothing in `CONTRACTS.md` needs touching.
- Every `spark-submit` invocation across all 4 call sites must get the
  **same** `HADOOP_CONF_DIR`/`YARN_CONF_DIR`/sizing flags — copy exactly,
  don't let them drift (there's no shared config file across PowerShell and
  Python in this repo, so this has to be kept consistent by hand; the
  design doc's "Ringkasan: flag lengkap gabungan §2–§4" block is the
  source of truth to copy from).
- `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` env vars (already set at every
  call site) must still be set **before** `spark-submit` runs, same as
  today — this migration adds `HADOOP_CONF_DIR`/`YARN_CONF_DIR` alongside
  them, doesn't replace that existing pattern.
- Verification is manual (`curl`, `jps`, browser, YARN RM UI at
  `http://localhost:8088`) — no automated test suite exists in this repo
  (established pattern, see `2026-07-10-history-refresh-button.md`'s same
  constraint).
- **Rollback per component, not all-or-nothing**: if `batch_job.py` works
  under YARN but `streaming_job.py` doesn't (or vice versa), it's fine to
  leave one on `local[*]` and the other on `yarn` — don't force both to
  move together if one is flaky.
- Commits go straight to `master` in `it-infra` (per project convention).
  This plan only touches `it-infra` — no `it-infra-web` changes, no
  subtree sync needed.
- Tasks 1–3 are **verification-only, no commit** (nothing they touch is
  tracked by git — `yarn-site.xml` lives in `C:\Hadoop\...`, outside both
  repos; manual terminal commands aren't files). Don't skip them to save
  time — they're the checkpoints that catch a broken YARN setup before it's
  baked into the scripts everyone (including `run_backend.ps1`) depends on.

---

### Task 1: Lower NodeManager memory cap, verify via RM REST API

**Repo / working directory:** none (edits `C:\Hadoop\hadoop-3.3.6\etc\hadoop\yarn-site.xml`, outside both git repos — no commit)

**Files:**
- Modify: `C:\Hadoop\hadoop-3.3.6\etc\hadoop\yarn-site.xml`

**Interfaces:**
- Consumes: nothing.
- Produces: NodeManager capacity that Tasks 2–6 depend on being 4096 MB, not the current 8192 MB default.

- [ ] **Step 1: Add the memory property**

Current `yarn-site.xml` only has the two `mapreduce_shuffle` aux-service
properties (no memory property at all — that's why it's at Hadoop's
built-in default of 8192 MB). Add inside the existing `<configuration>` block:

```xml
<property>
  <name>yarn.nodemanager.resource.memory-mb</name>
  <value>4096</value>
</property>
```

- [ ] **Step 2: Restart NodeManager**

NodeManager is currently running (confirmed via `jps` during design). Stop
and restart it so the new config takes effect — use whatever mechanism was
used to start it originally (check `scripts/start_infra.ps1` — if
NodeManager isn't started there, it was started manually and must be
restarted manually the same way, e.g. `yarn nodemanager` in its own
window, or via `Stop-Process`/relaunch if it's tracked by PID).

- [ ] **Step 3: Verify via RM REST API**

```bash
curl -s http://localhost:8088/ws/v1/cluster/metrics
```

Expected: `"totalMB":4096` (was `8192` before this task). If it still
shows `8192`, the NodeManager wasn't actually restarted with the new
config — don't proceed to Task 2 until this reads 4096.

---

### Task 2: Verify `batch_job.py` under YARN manually (no script changes yet)

**Repo / working directory:** `d:\Coding\#bigdata\it-infra` (verification-only, no commit)

**Files:** none modified.

**Interfaces:**
- Consumes: Task 1's 4096 MB NodeManager.
- Produces: confirmation that YARN submission actually works on this
  machine before Task 4 bakes it into `run_batch_daily.ps1` permanently.

- [ ] **Step 1: Run it manually with full YARN flags**

From `d:\Coding\#bigdata\it-infra`, in a shell where the shared venv's
`Scripts` dir is on PATH (same precondition as every other manual
`spark-submit` in this project):

```powershell
$venvPy = "D:\Coding\#bigdata\venv\Scripts\python.exe"
$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy
$env:HADOOP_CONF_DIR = "C:\Hadoop\hadoop-3.3.6\etc\hadoop"
$env:YARN_CONF_DIR = "C:\Hadoop\hadoop-3.3.6\etc\hadoop"

spark-submit --master yarn --deploy-mode client `
  --conf spark.executor.instances=1 `
  --conf spark.executor.memory=512m `
  --conf spark.executor.cores=1 `
  --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$venvPy" `
  --conf "spark.executorEnv.PYSPARK_PYTHON=$venvPy" `
  src\batch_job.py --date 2026-07-10
```

(2026-07-10 chosen because its expected result is already recorded:
`total_records=4404 unique_aircraft=593 busiest_hour=18` per
`what-have-done.md` and the local-mode run from the partition-overwrite
bugfix session.)

- [ ] **Step 2: Compare output**

Expected: `[batch_job] selesai: total_records=4404 unique_aircraft=593
busiest_hour=18 ...` — **identical** numbers to the local-mode run. If
different, something about the YARN execution path changed the data
(shouldn't happen — same code, same input — but verify rather than assume).

- [ ] **Step 3: Verify in the YARN RM UI**

Open `http://localhost:8088` in a browser. Expected: one application named
`opensky-batch`, state `FINISHED`, final status `SUCCEEDED`. Click into it,
confirm executor logs are viewable from there (this is the new place logs
live now — see design doc §Risiko poin 2).

- [ ] **Step 4: If this fails**

Don't debug by trial-and-error edits to this command. If it fails, stop and
apply `superpowers:systematic-debugging` before touching anything in Task
4+ — a broken YARN submission path debugged badly here will get copy-pasted
into 4 files in the next tasks. Common first things to check per the design
doc's §Risiko: is `winutils.exe`/`hadoop.dll` resolving correctly for this
specific code path (not just HDFS CLI, which already works); is
`HADOOP_CONF_DIR` actually pointing at a directory containing `yarn-site.xml`.

---

### Task 3: Verify `streaming_job.py` under YARN manually (no script changes yet)

**Repo / working directory:** `d:\Coding\#bigdata\it-infra` (verification-only, no commit)

**Files:** none modified.

**Interfaces:**
- Consumes: Task 2 passing cleanly first (don't attempt this if batch
  submission under YARN is still broken).
- Produces: confirmation that a long-running Structured Streaming app also
  works under YARN before Task 5 makes it permanent.

- [ ] **Step 1: Stop the currently-running local-mode `streaming_job.py`**

Check `jps` / the "Streaming Job" window from `run_backend.ps1` if it's
running — stop it cleanly first so there's only one instance writing to
the same MongoDB collections and HDFS checkpoint at a time.

- [ ] **Step 2: Run it manually with full YARN flags**

Same env vars as Task 2, Step 1, then:

```powershell
spark-submit --master yarn --deploy-mode client `
  --conf spark.executor.instances=1 `
  --conf spark.executor.memory=512m `
  --conf spark.executor.cores=1 `
  --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$venvPy" `
  --conf "spark.executorEnv.PYSPARK_PYTHON=$venvPy" `
  src\streaming_job.py
```

- [ ] **Step 3: Verify it's actually processing data**

Let it run a few minutes (ingest.py should still be running to feed it).
Check MongoDB directly (same verification pattern as
`what-have-done.md` §Track B): `live_states`, `zone_stats`, `alerts`
collections should keep updating. Check `http://localhost:8088` shows
`opensky-streaming` as `RUNNING` (not `FINISHED` — streaming apps stay
running).

- [ ] **Step 4: Verify checkpoint recovery still works**

This is the one thing `what-have-done.md` explicitly says was tested for
the local-mode version ("termasuk uji recovery — kill proses, restart dari
checkpoint, tidak ada duplikat/error") — re-verify it still holds under
YARN: kill the driver process (Ctrl+C in the terminal, or close the
window), restart with the same command, confirm it resumes from checkpoint
without duplicating data in `zone_stats`/`live_states`.

- [ ] **Step 5: If Task 2 passed but this fails**

That's a meaningful signal on its own — note in the commit message /
`what-have-done.md` entry (Task 7) whether the failure is specific to
long-running apps (e.g. container gets reclaimed after some idle timeout,
a YARN-specific behavior batch jobs wouldn't hit) vs a generic YARN
submission problem already ruled out by Task 2 passing.

---

### Task 4: Update `scripts/run_batch_daily.ps1`

**Repo / working directory:** `d:\Coding\#bigdata\it-infra`

**Files:**
- Modify: `scripts/run_batch_daily.ps1`

**Interfaces:**
- Consumes: Task 2 passing (don't do this until manual YARN batch
  submission is proven to work).
- Produces: the script used both for manual one-off runs and (after Task
  5) referenced by `run_backend.ps1`'s warning message on failure.

- [ ] **Step 1: Add `HADOOP_CONF_DIR`/`YARN_CONF_DIR` and YARN flags**

Replace:

```powershell
$env:PATH = "$venv\Scripts;" + $env:PATH
$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy

$sparkArgs = @("src\batch_job.py")
if ($Date) {
    $sparkArgs += @("--date", $Date)
}

& spark-submit @sparkArgs
```

With:

```powershell
$hadoopConf = "C:\Hadoop\hadoop-3.3.6\etc\hadoop"

$env:PATH = "$venv\Scripts;" + $env:PATH
$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy
$env:HADOOP_CONF_DIR = $hadoopConf
$env:YARN_CONF_DIR = $hadoopConf

# Lihat docs/design/2026-07-11-yarn-migration-design.md §Sizing executor
# utk alasan angka-angka ini (1 executor/aplikasi supaya batch_job.py dan
# streaming_job.py yang jalan bareng tidak melebihi budget NodeManager).
$sparkYarnArgs = @(
    "--master", "yarn",
    "--deploy-mode", "client",
    "--conf", "spark.executor.instances=1",
    "--conf", "spark.executor.memory=512m",
    "--conf", "spark.executor.cores=1",
    "--conf", "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$venvPy",
    "--conf", "spark.executorEnv.PYSPARK_PYTHON=$venvPy"
)

$sparkArgs = $sparkYarnArgs + @("src\batch_job.py")
if ($Date) {
    $sparkArgs += @("--date", $Date)
}

& spark-submit @sparkArgs
```

- [ ] **Step 2: Verify**

```powershell
scripts\run_batch_daily.ps1 -Date 2026-07-09
```

Expected: same clean success as Task 2, for a different date this time
(2026-07-09, also has a known-good local-mode result recorded in
`what-have-done.md`: `total_records=5660 unique_aircraft=624
busiest_hour=12`).

- [ ] **Step 3: Commit**

```bash
git add scripts/run_batch_daily.ps1
git commit -m "feat: run batch_job.py on YARN instead of local[*]"
```

---

### Task 5: Update `scripts/run_backend.ps1`

**Repo / working directory:** `d:\Coding\#bigdata\it-infra`

**Files:**
- Modify: `scripts/run_backend.ps1`

**Interfaces:**
- Consumes: Task 3 passing (streaming under YARN proven manually first).
- Produces: the one-command startup script now submits both the streaming
  job and the today's-date batch job to YARN; also sets
  `HADOOP_CONF_DIR`/`YARN_CONF_DIR` for every component's launcher
  (including the API's, which Task 6 depends on for its own
  subprocess-inherited environment).

- [ ] **Step 1: Add `$hadoopConf` and env vars to `Start-Component`'s template**

Near the top, next to `$venvPy`:

```powershell
$hadoopConf = "C:\Hadoop\hadoop-3.3.6\etc\hadoop"
```

In `Start-Component`, both the `cmd` branch and the `powershell` branch
currently set `PATH`/`PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` in the
generated launcher file. Add `HADOOP_CONF_DIR`/`YARN_CONF_DIR` alongside
them in **both** branches (uniformly for every component, same pattern as
the existing vars — harmless for components that don't use Spark, like
"API - uvicorn" and "Ingest", but Task 6 depends on the API's launcher
having these set so `api/main.py`'s subprocess call inherits them):

`cmd` branch — change:
```powershell
set "PATH=$venv\Scripts;%PATH%"
set "PYSPARK_PYTHON=$venvPy"
set "PYSPARK_DRIVER_PYTHON=$venvPy"
```
to:
```powershell
set "PATH=$venv\Scripts;%PATH%"
set "PYSPARK_PYTHON=$venvPy"
set "PYSPARK_DRIVER_PYTHON=$venvPy"
set "HADOOP_CONF_DIR=$hadoopConf"
set "YARN_CONF_DIR=$hadoopConf"
```

`powershell` branch — change:
```powershell
`$env:PATH = '$venv\Scripts;' + `$env:PATH
`$env:PYSPARK_PYTHON = '$venvPy'
`$env:PYSPARK_DRIVER_PYTHON = '$venvPy'
```
to:
```powershell
`$env:PATH = '$venv\Scripts;' + `$env:PATH
`$env:PYSPARK_PYTHON = '$venvPy'
`$env:PYSPARK_DRIVER_PYTHON = '$venvPy'
`$env:HADOOP_CONF_DIR = '$hadoopConf'
`$env:YARN_CONF_DIR = '$hadoopConf'
```

- [ ] **Step 2: Add YARN flags to the "Streaming Job" component**

Replace:
```powershell
Write-Output "=== 4/5: streaming_job.py (Spark Structured Streaming) [$Shell] ==="
Start-Component -Title "Streaming Job" -Exe "spark-submit" -Arguments "src\streaming_job.py"
```
with:
```powershell
Write-Output "=== 4/5: streaming_job.py (Spark Structured Streaming, YARN) [$Shell] ==="
$sparkYarnArgsStr = "--master yarn --deploy-mode client --conf spark.executor.instances=1 --conf spark.executor.memory=512m --conf spark.executor.cores=1 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=$venvPy --conf spark.executorEnv.PYSPARK_PYTHON=$venvPy"
Start-Component -Title "Streaming Job" -Exe "spark-submit" -Arguments "$sparkYarnArgsStr src\streaming_job.py"
```

- [ ] **Step 3: Add YARN flags to the "5/5" batch step**

Replace:
```powershell
Write-Output "=== 5/5: batch_job.py (agregasi harian, tanggal hari ini) ==="
$env:PATH = "$venv\Scripts;" + $env:PATH
$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy
$today = (Get-Date).ToString("yyyy-MM-dd")
try {
    & spark-submit "src\batch_job.py" --date $today
}
catch {
    Write-Warning "batch_job.py gagal untuk tanggal $today - tab History mungkin masih 404. Jalankan manual: scripts\run_batch_daily.ps1 -Date $today"
}
```
with:
```powershell
Write-Output "=== 5/5: batch_job.py (agregasi harian, tanggal hari ini, YARN) ==="
$env:PATH = "$venv\Scripts;" + $env:PATH
$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy
$env:HADOOP_CONF_DIR = $hadoopConf
$env:YARN_CONF_DIR = $hadoopConf
$today = (Get-Date).ToString("yyyy-MM-dd")
try {
    & spark-submit --master yarn --deploy-mode client `
        --conf spark.executor.instances=1 `
        --conf spark.executor.memory=512m `
        --conf spark.executor.cores=1 `
        --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$venvPy" `
        --conf "spark.executorEnv.PYSPARK_PYTHON=$venvPy" `
        "src\batch_job.py" --date $today
}
catch {
    Write-Warning "batch_job.py gagal untuk tanggal $today - tab History mungkin masih 404. Jalankan manual: scripts\run_batch_daily.ps1 -Date $today"
}
```

- [ ] **Step 4: Verify end-to-end**

Stop everything currently running (the streaming job left running from
Task 3, and anything `run_backend.ps1` would otherwise duplicate), then:

```powershell
scripts\run_backend.ps1
```

Expected: all 5 steps complete, "Streaming Job" window shows the
YARN-submitted driver output (not a `local[*]` startup banner — look for
`Connecting to ResourceManager` / a YARN application ID in the log), step
5/5's batch run succeeds same as Task 4. Check `http://localhost:8088`
shows both `opensky-streaming` (RUNNING) and, briefly during step 5,
`opensky-batch` (FINISHED once done) — this is the moment Task 3's
"do both fit in 4096 MB simultaneously" question gets a real answer, not
just the theoretical one from the design doc's math. If step 5 fails here
specifically (but passed alone in Task 4), that's a capacity problem —
see design doc §Sizing executor's guidance on turning `executor.memory`
down further before raising the NodeManager cap back up.

- [ ] **Step 5: Commit**

```bash
git add scripts/run_backend.ps1
git commit -m "feat: submit streaming_job.py and the daily batch step to YARN"
```

---

### Task 6: Update `api/main.py`'s `/api/history/refresh` subprocess call

**Repo / working directory:** `d:\Coding\#bigdata\it-infra`

**Files:**
- Modify: `api/main.py`

**Interfaces:**
- Consumes: Task 5 (the API's own launcher must already export
  `HADOOP_CONF_DIR`/`YARN_CONF_DIR` for this subprocess to inherit them —
  `asyncio.create_subprocess_exec` here doesn't pass an explicit `env=`,
  so it inherits whatever environment the API process itself was started
  with).
- Produces: `POST /api/history/refresh` now submits `batch_job.py` to YARN
  too, consistent with the other 3 call sites.

- [ ] **Step 1: Add YARN flags to the subprocess args**

In `history_refresh()`, replace:

```python
            proc = await asyncio.create_subprocess_exec(
                "spark-submit.cmd",
                "src/batch_job.py",
                "--date",
                date,
                cwd=str(repo_root),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
```

with:

```python
            proc = await asyncio.create_subprocess_exec(
                "spark-submit.cmd",
                "--master", "yarn",
                "--deploy-mode", "client",
                "--conf", "spark.executor.instances=1",
                "--conf", "spark.executor.memory=512m",
                "--conf", "spark.executor.cores=1",
                "--conf", f"spark.yarn.appMasterEnv.PYSPARK_PYTHON={sys.executable}",
                "--conf", f"spark.executorEnv.PYSPARK_PYTHON={sys.executable}",
                "src/batch_job.py",
                "--date",
                date,
                cwd=str(repo_root),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
```

`sys.executable` works here without a hardcoded path because `api/main.py`
itself is always launched via the shared venv's `python.exe` (see
`run_backend.ps1`'s `Start-Component -Title "API - uvicorn" -Exe $venvPy
...`) — `sys` is already imported at the top of this file.

- [ ] **Step 2: Restart the API and verify**

The API process must be restarted (picking up code changes, and — if this
is the first restart since Task 5's script edit — the new
`HADOOP_CONF_DIR`/`YARN_CONF_DIR` env vars from its own launcher):

```powershell
# stop the current "API - uvicorn" window/process, then:
scripts\.launch\API_-_uvicorn.cmd
```
(or restart via `run_backend.ps1` if starting everything fresh)

```bash
curl -s -X POST "http://localhost:8000/api/history/refresh?date=2026-07-11" -w "\nSTATUS:%{http_code}\n"
```

Expected: `200 {"date":"2026-07-11","status":"ok"}`, same as before this
migration — from the outside, this endpoint's contract is unchanged (per
Global Constraints, no `CONTRACTS.md` update needed). Check
`http://localhost:8088` shows the `opensky-batch` application again,
confirming it went through YARN this time, not `local[*]`.

- [ ] **Step 3: Commit**

```bash
git add api/main.py
git commit -m "feat: submit /api/history/refresh's batch_job.py to YARN"
```

---

### Task 7: Update `README.md` and `what-have-done.md`

**Repo / working directory:** `d:\Coding\#bigdata\it-infra`

**Files:**
- Modify: `README.md`
- Modify: `what-have-done.md`

**Interfaces:**
- Consumes: the outcome of Tasks 1–6 (including any deviations — e.g. if
  Task 3 revealed `streaming_job.py` needed to stay on `local[*]`, that
  goes here, not silently dropped).
- Produces: nothing consumed elsewhere — documentation only, last task.

- [ ] **Step 1: `README.md`**

Add a short section (near wherever `run_backend.ps1`/environment setup is
currently documented) noting: Spark jobs now run via YARN, requires
`HADOOP_CONF_DIR`/`YARN_CONF_DIR` pointing at
`C:\Hadoop\hadoop-3.3.6\etc\hadoop`, ResourceManager UI at
`http://localhost:8088` for checking job status/logs (this is the new
place to look — see design doc §Risiko poin 2 for why the old
"just read the terminal window" habit doesn't fully apply to executor logs
anymore).

- [ ] **Step 2: `what-have-done.md`**

Append a dated session entry (`2026-07-11` or whenever this is actually
executed) summarizing: what changed (4 call sites, `yarn-site.xml` memory
cap), what was verified (Tasks 2–3's manual checks, Task 5's concurrent
capacity check), and **be honest about anything that didn't go as planned**
— e.g. if `spark.executor.memory=512m` had to be lowered further, if
Task 3's checkpoint-recovery check behaved differently under YARN than
local mode, if NodeManager needed more restarts than expected. This
project's existing log entries (e.g. the partition-overwrite bug, the
`--reload`/`NotImplementedError` bug) consistently document what actually
went wrong, not just the happy path — keep that pattern.

- [ ] **Step 3: Commit**

```bash
git add README.md what-have-done.md
git commit -m "docs: log YARN migration session in what-have-done.md, document setup in README"
```

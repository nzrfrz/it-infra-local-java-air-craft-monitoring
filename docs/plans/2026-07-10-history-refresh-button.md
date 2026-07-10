# History "Update" Button Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the user re-run `batch_job.py` for a chosen date from the History tab web UI instead of running `scripts\run_batch_daily.ps1` manually.

**Architecture:** One new blocking REST endpoint (`POST /api/history/refresh?date=YYYY-MM-DD`) on the existing FastAPI backend runs `spark-submit src/batch_job.py --date <date>` as an awaited `asyncio` subprocess and returns once it finishes. One new "UPDATE" button in `HistoryView.tsx` calls it, then re-fetches the three History panels.

**Tech Stack:** FastAPI (Python, `it-infra/api/main.py`), React + TypeScript (`it-infra-web/src`). No new dependencies.

## Global Constraints

- No path/credential hardcoding — endpoint takes `date` from the query string only, everything else (venv, `spark-submit` on PATH) is inherited from the process environment exactly like the other Spark components launched by `scripts/run_backend.ps1` (see `it-infra/scripts/run_backend.ps1:52-74`).
- Blocking request/response only — no background job/polling, no WebSocket progress push (confirmed with user during design).
- Only one batch refresh runs at a time (in-memory lock; single dev-server process, not distributed-safe — acceptable for this solo-user local pipeline).
- Design doc: `it-infra/docs/design/2026-07-10-history-refresh-button-design.md`. Contract addition documented in `it-infra/docs/design/CONTRACTS.md` C3.
- **No automated test suite exists in either repo** (`it-infra` has no `pytest`/`conftest.py` anywhere; `it-infra-web` has no test runner in `package.json`). Following the codebase's established pattern, verification steps in this plan are manual (`curl` for the backend, browser click for the frontend) rather than introducing a new test framework for one endpoint.
- Commits go straight to `master` in each repo (per project convention — solo sequential work, no branches). `it-infra` and `it-infra-web` are **separate git repositories** — Task 1-2 commit in `it-infra`, Task 3-4 commit in `it-infra-web`. Pay attention to the working directory named at the top of each task.

---

### Task 1: Backend — `POST /api/history/refresh` endpoint

**Repo / working directory:** `d:\Coding\#bigdata\it-infra`

**Files:**
- Modify: `api/main.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `POST /api/history/refresh?date=YYYY-MM-DD` → `200 {"date": str, "status": "ok"}` / `400` (bad date format) / `409` (refresh already running) / `500` (batch job failed, `detail` = trimmed stderr). This is what Task 3's `refreshHistory()` calls.

- [ ] **Step 1: Add the imports and module-level lock**

Open `api/main.py`. At the top, `import asyncio` and `import re` are already partially present (`asyncio` is imported at line 8; `re` is not). Add `re` to the existing import block:

```python
import asyncio
import re
import sys
import threading
import time
```

Right after the `_WATCHED_COLLECTIONS = {"live_states", "zone_stats", "alerts"}` line, add:

```python
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_refresh_lock = asyncio.Lock()
```

- [ ] **Step 2: Add the endpoint**

Add this function directly after `history_density` (i.e. after the `return {"date": date, "cells": cells}` line, before the `@app.get("/api/live/states")` block):

```python
@app.post("/api/history/refresh")
async def history_refresh(date: str):
    if not _DATE_RE.match(date):
        raise HTTPException(status_code=400, detail="date harus format YYYY-MM-DD")

    if _refresh_lock.locked():
        raise HTTPException(status_code=409, detail="Refresh lain sedang berjalan, coba lagi sebentar")

    async with _refresh_lock:
        repo_root = Path(__file__).resolve().parent.parent
        proc = await asyncio.create_subprocess_exec(
            "spark-submit",
            "src/batch_job.py",
            "--date",
            date,
            cwd=str(repo_root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()

    if proc.returncode != 0:
        tail = stderr.decode(errors="replace")[-2000:]
        raise HTTPException(status_code=500, detail=f"batch_job.py gagal (exit {proc.returncode}): {tail}")

    return {"date": date, "status": "ok"}
```

Note the `if _refresh_lock.locked():` check and the `async with _refresh_lock:` acquire have no `await` between them, so they're atomic under asyncio's single-threaded cooperative scheduling — no separate `try_acquire` needed.

- [ ] **Step 3: Verify the endpoint manually**

The backend must be running via `scripts\run_backend.ps1` (or at least started with the same PATH/`PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` env vars — see `scripts/run_backend.ps1:52-74`) so that `spark-submit` resolves and Spark uses the shared venv's Python. If it's not already running, start it:

```powershell
scripts\run_backend.ps1
```

Wait for the "API - uvicorn" window to show `Application startup complete`, then from any shell:

```bash
curl -s -X POST "http://localhost:8000/api/history/refresh?date=2026-07-10"
```

Expected: after ~10-40s, `{"date":"2026-07-10","status":"ok"}`. If it 500s, read the `detail` field — likely means no raw HDFS data exists for that date yet (expected if `ingest.py` hasn't run today) or HDFS is down; that's the endpoint doing its job correctly, not a bug in this task.

Also verify the lock: fire two requests back-to-back for different dates —

```bash
curl -s -X POST "http://localhost:8000/api/history/refresh?date=2026-07-09" &
curl -s -X POST "http://localhost:8000/api/history/refresh?date=2026-07-10" &
wait
```

Expected: one returns `200`, the other `409 {"detail":"Refresh lain sedang berjalan, coba lagi sebentar"}`.

And verify date validation:

```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST "http://localhost:8000/api/history/refresh?date=not-a-date"
```

Expected: `400`.

- [ ] **Step 4: Commit**

```bash
git add api/main.py
git commit -m "feat: add POST /api/history/refresh to trigger batch_job.py from the API"
```

---

### Task 2: Update `CONTRACTS.md` C3

**Repo / working directory:** `d:\Coding\#bigdata\it-infra`

**Files:**
- Modify: `docs/design/CONTRACTS.md`

**Interfaces:**
- Consumes: the endpoint contract from Task 1.
- Produces: nothing consumed by later tasks — documentation only.

- [ ] **Step 1: Add the new line to the C3 block**

In `docs/design/CONTRACTS.md`, find the C3 code block (currently ending with the `WS /ws/live` line). Add a new line directly after it:

```
GET /api/history/summary?date=YYYY-MM-DD   → dokumen daily_snapshot (404 jika belum ada)
GET /api/history/hourly?date=YYYY-MM-DD    → {date, hours:[{hour:0-23, aircraft_count, avg_velocity_ms}]}
GET /api/history/density?date=YYYY-MM-DD   → {date, cells:[{zone:"-7_110", lat:-7, lon:110, count}]}
POST /api/history/refresh?date=YYYY-MM-DD  → jalankan batch_job.py utk tanggal itu (blocking, ~30s), balas {date, status:"ok"} (400 date invalid, 409 refresh lain masih jalan, 500 batch gagal)
GET /api/live/states                       → {states:[<dokumen live_states>]}  (snapshot awal utk frontend)
WS  /ws/live                               → pesan JSON: {"channel":"live_states"|"zone_stats"|"alerts"|"snapshot", "data":{<dokumen>}}
```

- [ ] **Step 2: Commit**

```bash
git add docs/design/CONTRACTS.md
git commit -m "docs: document POST /api/history/refresh in contract C3"
```

---

### Task 3: Frontend — `refreshHistory()` API client function

**Repo / working directory:** `d:\Coding\Projects\it-infra-web`

**Files:**
- Modify: `src/lib/api.ts`

**Interfaces:**
- Consumes: the endpoint from Task 1 (`POST /api/history/refresh?date=...`).
- Produces: `refreshHistory(date: string): Promise<{ date: string; status: string }>`, throwing `ApiError` on non-2xx — this is what Task 4's `HistoryView.tsx` calls.

- [ ] **Step 1: Add a `postJson` helper next to the existing `getJson`**

In `src/lib/api.ts`, directly after the closing brace of `getJson`, add:

```typescript
async function postJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { method: "POST" });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new ApiError(res.status, body.detail ?? res.statusText);
  }
  return res.json();
}
```

- [ ] **Step 2: Add `refreshHistory`**

Directly after `fetchDensity`, add:

```typescript
export function refreshHistory(date: string) {
  return postJson<{ date: string; status: string }>(`/api/history/refresh?date=${date}`);
}
```

- [ ] **Step 3: Verify it type-checks**

```bash
npx tsc -b
```

Expected: exits 0, no errors.

- [ ] **Step 4: Commit**

```bash
git add src/lib/api.ts
git commit -m "feat: add refreshHistory() API client for POST /api/history/refresh"
```

---

### Task 4: Frontend — "UPDATE" button in `HistoryView.tsx`

**Repo / working directory:** `d:\Coding\Projects\it-infra-web`

**Files:**
- Modify: `src/components/HistoryView.tsx`

**Interfaces:**
- Consumes: `refreshHistory(date: string)` from Task 3, existing `fetchSummary`, `fetchHourly`, `fetchDensity`, `ApiError`, `describeError` already in this file.
- Produces: nothing consumed elsewhere — this is the final UI-visible task.

- [ ] **Step 1: Import `refreshHistory` and add `useCallback`**

Change the import line at the top of `src/components/HistoryView.tsx`:

```typescript
import { useCallback, useEffect, useState, type ReactNode } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { ApiError, fetchDensity, fetchHourly, fetchSummary, refreshHistory } from "../lib/api";
import type { DailySummary, DensityCell, HourlyBucket } from "../types";
import { DensityHeatmap } from "./DensityHeatmap";
```

- [ ] **Step 2: Extract the fetch logic into a reusable `loadAll` function and add refresh state**

Replace the whole `useEffect` block (currently lines 19-35, from `useEffect(() => {` through the closing `}, [date]);`) with:

```typescript
  const loadAll = useCallback((d: string) => {
    setSummary({ loading: true, error: null, data: null });
    setHourly({ loading: true, error: null, data: null });
    setDensity({ loading: true, error: null, data: null });

    fetchSummary(d)
      .then((data) => setSummary({ loading: false, error: null, data }))
      .catch((e) => setSummary({ loading: false, error: describeError(e), data: null }));

    fetchHourly(d)
      .then((r) => setHourly({ loading: false, error: null, data: r.hours }))
      .catch((e) => setHourly({ loading: false, error: describeError(e), data: null }));

    fetchDensity(d)
      .then((r) => setDensity({ loading: false, error: null, data: r.cells }))
      .catch((e) => setDensity({ loading: false, error: describeError(e), data: null }));
  }, []);

  useEffect(() => {
    loadAll(date);
  }, [date, loadAll]);

  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);

  const handleRefresh = useCallback(async () => {
    setRefreshing(true);
    setRefreshError(null);
    try {
      await refreshHistory(date);
      loadAll(date);
    } catch (e) {
      setRefreshError(e instanceof ApiError ? e.message : "Gagal menjalankan refresh. Cek apakah API dan Spark/HDFS sedang berjalan.");
    } finally {
      setRefreshing(false);
    }
  }, [date, loadAll]);
```

This keeps `fetchSummary`/`fetchHourly`/`fetchDensity` calls byte-for-byte identical to what they were in the original effect — only the wrapping changed (extracted into `loadAll`, parameterized by `d` instead of closing over `date` directly, so both the effect and the button call the same logic without duplicating it).

- [ ] **Step 3: Add the button next to the date input**

Find the `TANGGAL` input block:

```tsx
      <div className="flex shrink-0 items-center gap-3 self-start rounded border border-hairline bg-panel px-3 py-2 font-mono text-xs">
        <label className="tracking-widest text-text-dim">TANGGAL</label>
        <input
          type="date"
          value={date}
          max={todayIso()}
          onChange={(e) => setDate(e.target.value)}
          className="w-[9.5rem] rounded border border-hairline bg-void px-2 py-1 text-text [color-scheme:dark]"
        />
      </div>
```

Replace it with (adds the button + inline error message, same wrapper):

```tsx
      <div className="flex shrink-0 items-center gap-3 self-start rounded border border-hairline bg-panel px-3 py-2 font-mono text-xs">
        <label className="tracking-widest text-text-dim">TANGGAL</label>
        <input
          type="date"
          value={date}
          max={todayIso()}
          onChange={(e) => setDate(e.target.value)}
          className="w-[9.5rem] rounded border border-hairline bg-void px-2 py-1 text-text [color-scheme:dark]"
        />
        <button
          type="button"
          disabled={refreshing}
          onClick={handleRefresh}
          className="rounded border border-hairline bg-void px-3 py-1 tracking-widest text-text-dim hover:text-phosphor disabled:cursor-not-allowed disabled:opacity-50"
        >
          {refreshing ? "MEMPROSES..." : "UPDATE"}
        </button>
        {refreshError && <span className="text-[10px] text-red-400">{refreshError}</span>}
      </div>
```

- [ ] **Step 4: Verify it type-checks and lints**

```bash
npx tsc -b
npx oxlint src/components/HistoryView.tsx
```

Expected: both exit 0, no errors.

- [ ] **Step 5: Manual verification in the browser**

With the backend running (Task 1's `scripts\run_backend.ps1`) and `npm run dev` running in `it-infra-web`, open the History tab:

1. Click "UPDATE" without changing the date. Expected: button text changes to "MEMPROSES...", is disabled, and after ~10-40s reverts to "UPDATE" with the three panels' data refreshed (loading spinners flash then repopulate).
2. Change the date picker to a date with no raw HDFS data (e.g. a date far in the past before `ingest.py` ever ran), click "UPDATE". Expected: after the wait, an inline red error message appears next to the button (the trimmed `batch_job.py` stderr / 404-style message), and the panels below still show whatever they showed before (or their own "belum tersedia" state if this was the first load for that date).
3. Click "UPDATE" twice in quick succession. Expected: second click is a no-op while `refreshing` is true (button is disabled, so this can't actually double-fire from the UI — this step just confirms the disabled state holds).

- [ ] **Step 6: Commit**

```bash
git add src/components/HistoryView.tsx
git commit -m "feat: add UPDATE button to History tab to trigger batch refresh"
```

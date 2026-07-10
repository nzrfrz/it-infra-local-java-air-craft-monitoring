# Design: History "Update" Button (trigger batch_job.py from web)

## Problem

History tab data (`/api/history/summary`, `/hourly`, `/density`) only reflects
whatever `batch_job.py` last wrote for that date. Today's data is refreshed
automatically once, at backend startup (`run_backend.ps1` step 5/5). Any other
date, or a re-run for today to pick up newly-ingested records later in the
day, requires the user to manually run
`scripts\run_batch_daily.ps1 -Date YYYY-MM-DD` in a terminal.

Goal: let the user trigger this from the web UI instead, via a button next to
the date picker in `HistoryView.tsx`.

## Approach

Add a new REST endpoint that runs `batch_job.py` for a given date and blocks
until it finishes (~30s), plus an "Update" button in the frontend that calls
it and then re-fetches the three History panels.

Considered and rejected: background job + polling (job-id, status endpoint).
Overkill for a solo-user local pipeline — one blocking call with a disabled
button + spinner is simpler and sufficient (confirmed with user).

## Backend — new endpoint (extends contract C3)

```
POST /api/history/refresh?date=YYYY-MM-DD
  -> runs `spark-submit src/batch_job.py --date <date>` as a subprocess,
     awaited (async, does not block the event loop / /ws/live stream)
  -> 200 {"date": "...", "status": "ok"}
  -> 409 {"detail": "Refresh sedang berjalan untuk tanggal lain"}  if a refresh
     is already in progress (module-level lock/flag; only one batch job runs
     at a time)
  -> 500 {"detail": "<tail of stderr>"}  if batch_job.py exits non-zero
```

Implementation notes (`api/main.py`):
- Reuse the same venv/env-var pattern already used to launch Spark components
  (`PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON`, `spark-submit` on PATH) — the API
  process inherits these from the launcher script that starts uvicorn, so no
  extra config needed.
- `asyncio.create_subprocess_exec("spark-submit", "src/batch_job.py",
  "--date", date, ...)`, `await proc.communicate()`, check `proc.returncode`.
- A simple in-memory `asyncio.Lock`-guarded flag (`_refresh_in_progress: bool`)
  covers the single-process dev server. Not distributed-safe, not needed here.
- Validate `date` matches `YYYY-MM-DD` (reuse existing date-string convention
  from the other `/api/history/*` endpoints — no stricter validation exists
  today, so match that).

Update `docs/design/CONTRACTS.md` C3 section to document the new endpoint.

## Frontend — Update button (`it-infra-web/src/components/HistoryView.tsx`)

- New button next to the `TANGGAL` date input: label "UPDATE", disabled +
  label "MEMPROSES..." while a request is in flight.
- `src/lib/api.ts`: add `refreshHistory(date: string)` → `POST
  /api/history/refresh?date=...`, throws `ApiError` on non-2xx (same pattern
  as `getJson`).
- On click: call `refreshHistory(date)`; on success, re-run the same
  summary/hourly/density fetch the `useEffect` already does (extract that
  fetch logic into a function so both the effect and the button can call it,
  rather than duplicating it).
- On failure: show the error message inline near the button (small red/dim
  text, cleared on next click) — same tone as the existing
  `describeError`-based panel error states, not a toast/modal (no such
  pattern exists yet in this app).
- Button has no separate loading state library — a local `refreshing: boolean`
  plus `refreshError: string | null` in `HistoryView` state is enough.

## Error handling

- `batch_job.py` failure (e.g. no raw HDFS data for that date, HDFS down):
  surfaced as the 500 detail text, shown inline by the button.
- Double-click / concurrent refresh for a different date while one is running:
  409, shown the same way ("Refresh lain sedang berjalan, coba lagi
  sebentar").
- No change to existing 404 behavior for dates that have never been batched —
  that's still how the panels report "belum tersedia" before the user ever
  clicks Update.

## Out of scope

- No auth/permission check (matches rest of this local-only API).
- No websocket-based progress push — plain blocking request/response.
- No retry/backoff on the frontend — one click, one attempt, error shown on
  failure, user can just click again.

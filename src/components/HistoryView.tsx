import { useCallback, useEffect, useState, type ReactNode } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { ApiError, fetchDensity, fetchHourly, fetchSummary, refreshHistory } from "../lib/api";
import type { DailySummary, DensityCell, HourlyBucket } from "../types";
import { DensityHeatmap } from "./DensityHeatmap";

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

type Load<T> = { loading: boolean; error: string | null; data: T | null };

export function HistoryView() {
  const [date, setDate] = useState(todayIso());
  const [summary, setSummary] = useState<Load<DailySummary>>({ loading: true, error: null, data: null });
  const [hourly, setHourly] = useState<Load<HourlyBucket[]>>({ loading: true, error: null, data: null });
  const [density, setDensity] = useState<Load<DensityCell[]>>({ loading: true, error: null, data: null });

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

  return (
    <div className="flex h-full flex-col gap-3 overflow-y-auto p-3">
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

      <div className="grid shrink-0 grid-cols-3 gap-3">
        <section className="col-span-1 flex max-h-[280px] flex-col rounded border border-hairline bg-panel p-3">
          <h2 className="mb-2 shrink-0 font-mono text-xs tracking-widest text-text-dim">RINGKASAN HARIAN</h2>
          <div className="min-h-0 overflow-y-auto">
            <Panel state={summary} emptyLabel="daily_snapshot belum tersedia untuk tanggal ini">
              {(s) => (
                <div className="font-mono text-sm">
                  <div className="grid grid-cols-2 gap-y-2">
                    <span className="text-text-dim">Total record</span>
                    <span className="text-right text-phosphor">{s.total_records.toLocaleString("id-ID")}</span>
                    <span className="text-text-dim">Pesawat unik</span>
                    <span className="text-right text-phosphor">{s.unique_aircraft.toLocaleString("id-ID")}</span>
                    <span className="text-text-dim">Jam tersibuk</span>
                    <span className="text-right text-phosphor">{s.busiest_hour ?? "--"}:00</span>
                  </div>
                  <div className="mt-3 border-t border-hairline pt-2">
                    <span className="text-[10px] tracking-widest text-text-dim">TOP MASKAPAI (PREFIX CALLSIGN)</span>
                    <ul className="mt-1 space-y-0.5 pb-1">
                      {s.top_airlines.slice(0, 5).map((a) => (
                        <li key={a.prefix} className="flex justify-between">
                          <span>{a.prefix}</span>
                          <span className="text-text-dim">{a.count}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}
            </Panel>
          </div>
        </section>

        <section className="col-span-2 max-h-[280px] rounded border border-hairline bg-panel p-3">
          <h2 className="mb-2 font-mono text-xs tracking-widest text-text-dim">PESAWAT PER JAM</h2>
          <Panel state={hourly} emptyLabel="Parquet states_clean belum tersedia untuk tanggal ini">
            {(hours) => (
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={hours}>
                  <CartesianGrid stroke="#223042" vertical={false} />
                  <XAxis dataKey="hour" tick={{ fill: "#7b8ba1", fontSize: 11 }} tickLine={false} axisLine={{ stroke: "#223042" }} />
                  <YAxis tick={{ fill: "#7b8ba1", fontSize: 11 }} tickLine={false} axisLine={{ stroke: "#223042" }} />
                  <Tooltip
                    contentStyle={{ background: "#111820", border: "1px solid #223042", fontFamily: "IBM Plex Mono" }}
                    labelStyle={{ color: "#e6edf3" }}
                    cursor={{ fill: "#4ade8022" }}
                  />
                  <Bar dataKey="aircraft_count" fill="#4ade80" radius={[2, 2, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </Panel>
        </section>
      </div>

      <section className="flex min-h-[300px] flex-1 flex-col overflow-hidden rounded border border-hairline bg-panel p-3">
        <h2 className="mb-2 shrink-0 font-mono text-xs tracking-widest text-text-dim">KEPADATAN ZONA (GRID 1°×1°)</h2>
        <div className="min-h-0 flex-1">
          <Panel state={density} emptyLabel="Parquet density_grid_hourly belum tersedia untuk tanggal ini">
            {(cells) => <DensityHeatmap cells={cells} />}
          </Panel>
        </div>
      </section>
    </div>
  );
}

function describeError(e: unknown): string {
  if (e instanceof ApiError && e.status === 404) return e.message;
  return "Gagal memuat data. Cek apakah API dan HDFS/MongoDB sedang berjalan.";
}

function Panel<T>({
  state,
  emptyLabel,
  children,
}: {
  state: Load<T>;
  emptyLabel: string;
  children: (data: T) => ReactNode;
}) {
  if (state.loading) return <div className="font-mono text-xs text-text-dim">Memuat...</div>;
  if (state.error) return <div className="font-mono text-xs text-text-dim">{state.error || emptyLabel}</div>;
  if (!state.data) return <div className="font-mono text-xs text-text-dim">{emptyLabel}</div>;
  return <>{children(state.data)}</>;
}

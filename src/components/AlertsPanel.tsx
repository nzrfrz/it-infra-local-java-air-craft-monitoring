import type { Alert } from "../types";

function timeAgo(ts: number) {
  const diff = Math.max(0, Date.now() / 1000 - ts);
  if (diff < 60) return `${Math.round(diff)}s lalu`;
  if (diff < 3600) return `${Math.round(diff / 60)}m lalu`;
  return `${Math.round(diff / 3600)}j lalu`;
}

export function AlertsPanel({ alerts }: { alerts: Alert[] }) {
  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between border-b border-hairline px-3 py-2">
        <span className="font-mono text-xs tracking-widest text-text-dim">ALERT LOG</span>
        <span className="font-mono text-xs text-text-dim">{alerts.length}</span>
      </div>
      <div className="flex-1 overflow-y-auto">
        {alerts.length === 0 && (
          <div className="p-3 font-mono text-xs text-text-dim">Belum ada alert. Semua jalur bersih.</div>
        )}
        {alerts.map((a, i) => {
          const critical = a.severity === "critical";
          return (
            <div
              key={`${a.ts}-${i}`}
              className={`border-b border-hairline/60 px-3 py-2 text-xs ${critical ? "bg-critical/10" : ""}`}
            >
              <div className="flex items-center gap-2">
                <span
                  className={`rounded px-1.5 py-0.5 font-mono text-[10px] font-semibold tracking-wide ${
                    critical ? "bg-critical text-void" : "bg-amber text-void"
                  }`}
                >
                  {critical ? "CRITICAL" : "WARNING"}
                </span>
                <span className="font-mono text-text-dim">{timeAgo(a.ts)}</span>
              </div>
              <div className="mt-1 font-mono text-text">
                {a.type === "emergency_squawk" ? `SQUAWK · ${a.icao24}` : `DENSITY SPIKE · zona ${a.zone}`}
              </div>
              <div className="text-text-dim">{a.details}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

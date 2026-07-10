import type { ConnectionStatus } from "../hooks/useLiveSocket";

interface Props {
  status: ConnectionStatus;
  aircraftCount: number;
  lastUpdate: number | null;
  view: "live" | "history";
  onViewChange: (v: "live" | "history") => void;
}

const STATUS_LABEL: Record<ConnectionStatus, string> = {
  connecting: "MENYAMBUNG",
  open: "LIVE",
  closed: "TERPUTUS",
};

const STATUS_COLOR: Record<ConnectionStatus, string> = {
  connecting: "bg-amber",
  open: "bg-phosphor",
  closed: "bg-critical",
};

export function StatusHeader({ status, aircraftCount, lastUpdate, view, onViewChange }: Props) {
  return (
    <header className="flex items-center justify-between border-b border-hairline bg-panel px-4 py-3">
      <div className="flex items-center gap-3">
        <div className="flex flex-col leading-none">
          <span className="font-mono text-[15px] font-semibold tracking-[0.15em] text-text">STATION JAVA-NAS</span>
          <span className="mt-1 font-mono text-[10px] tracking-widest text-text-dim">
            MONITORING LALU LINTAS UDARA · INDONESIA
          </span>
        </div>
      </div>

      <nav className="flex items-center gap-1 rounded border border-hairline bg-void p-1 font-mono text-xs">
        {(["live", "history"] as const).map((v) => (
          <button
            key={v}
            onClick={() => onViewChange(v)}
            className={`rounded px-3 py-1 tracking-widest transition-colors ${
              view === v ? "bg-phosphor-dim text-void" : "text-text-dim hover:text-text"
            }`}
          >
            {v === "live" ? "LIVE" : "HISTORY"}
          </button>
        ))}
      </nav>

      <div className="flex items-center gap-5 font-mono text-xs text-text-dim">
        <div className="flex flex-col items-end leading-none">
          <span className="text-text">{aircraftCount}</span>
          <span className="mt-1 text-[10px] tracking-widest">PESAWAT</span>
        </div>
        <div className="flex flex-col items-end leading-none">
          <span className="text-text">{lastUpdate ? new Date(lastUpdate).toLocaleTimeString("id-ID") : "--:--:--"}</span>
          <span className="mt-1 text-[10px] tracking-widest">UPDATE TERAKHIR</span>
        </div>
        <div className="flex items-center gap-2">
          <span className={`h-2 w-2 rounded-full ${STATUS_COLOR[status]} ${status === "open" ? "animate-pulse-dot" : ""}`} />
          <span className="tracking-widest">{STATUS_LABEL[status]}</span>
        </div>
      </div>
    </header>
  );
}

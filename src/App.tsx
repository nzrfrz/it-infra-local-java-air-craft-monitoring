import { useState } from "react";
import { AlertsPanel } from "./components/AlertsPanel";
import { HistoryView } from "./components/HistoryView";
import { LiveMap } from "./components/LiveMap";
import { StatusHeader } from "./components/StatusHeader";
import { useLiveSocket } from "./hooks/useLiveSocket";
import { WS_URL } from "./lib/api";

function App() {
  const [view, setView] = useState<"live" | "history">("live");
  const { status, states, alerts, lastUpdate } = useLiveSocket(WS_URL);

  return (
    <div className="flex h-screen flex-col bg-void text-text">
      <StatusHeader
        status={status}
        aircraftCount={states.size}
        lastUpdate={lastUpdate}
        view={view}
        onViewChange={setView}
      />

      <main className="flex-1 overflow-hidden">
        {view === "live" ? (
          <div className="grid h-full grid-cols-[1fr_320px]">
            <LiveMap states={Array.from(states.values())} />
            <div className="border-l border-hairline bg-panel">
              <AlertsPanel alerts={alerts} />
            </div>
          </div>
        ) : (
          <HistoryView />
        )}
      </main>
    </div>
  );
}

export default App;

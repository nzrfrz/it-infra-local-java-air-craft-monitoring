import { useEffect, useRef, useState } from "react";
import type { Alert, LiveState, WsMessage, ZoneStat } from "../types";

export type ConnectionStatus = "connecting" | "open" | "closed";

const MAX_ALERTS = 50;
const MAX_BACKOFF_MS = 15_000;

export function useLiveSocket(url: string) {
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [states, setStates] = useState<Map<string, LiveState>>(new Map());
  const [zoneStats, setZoneStats] = useState<Map<string, ZoneStat>>(new Map());
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [lastUpdate, setLastUpdate] = useState<number | null>(null);

  const backoffRef = useRef(500);
  const socketRef = useRef<WebSocket | null>(null);
  const closedByUsRef = useRef(false);

  useEffect(() => {
    closedByUsRef.current = false;

    function connect() {
      setStatus("connecting");
      const ws = new WebSocket(url);
      socketRef.current = ws;

      ws.onopen = () => {
        setStatus("open");
        backoffRef.current = 500;
      };

      ws.onmessage = (event) => {
        const msg: WsMessage = JSON.parse(event.data);
        setLastUpdate(Date.now());

        if (msg.channel === "snapshot") {
          setStates(new Map(msg.data.states.map((s) => [s.icao24, s])));
        } else if (msg.channel === "live_states") {
          setStates((prev) => {
            const next = new Map(prev);
            next.set(msg.data.icao24, msg.data);
            return next;
          });
        } else if (msg.channel === "zone_stats") {
          setZoneStats((prev) => {
            const next = new Map(prev);
            next.set(msg.data._id, msg.data);
            return next;
          });
        } else if (msg.channel === "alerts") {
          setAlerts((prev) => [msg.data, ...prev].slice(0, MAX_ALERTS));
        }
      };

      ws.onclose = () => {
        setStatus("closed");
        if (closedByUsRef.current) return;
        const delay = backoffRef.current;
        backoffRef.current = Math.min(delay * 2, MAX_BACKOFF_MS);
        setTimeout(connect, delay);
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();

    return () => {
      closedByUsRef.current = true;
      socketRef.current?.close();
    };
  }, [url]);

  return { status, states, zoneStats, alerts, lastUpdate };
}

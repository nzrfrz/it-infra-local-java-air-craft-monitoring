import { DeckGL } from "@deck.gl/react";
import { IconLayer } from "@deck.gl/layers";
import { useMemo, useState } from "react";
import { Map as MapLibreMap } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import type { LiveState } from "../types";
import { RadarSweep } from "./RadarSweep";

const EMERGENCY_SQUAWKS = new Set(["7500", "7600", "7700"]);

const INITIAL_VIEW_STATE = {
  longitude: 109.5,
  latitude: -6.8,
  zoom: 5.4,
  pitch: 0,
  bearing: 0,
};

// Segitiga pesawat, dibangkitkan sebagai data-URI SVG (tanpa aset eksternal).
function aircraftIcon(color: string) {
  const svg = `<svg xmlns='http://www.w3.org/2000/svg' width='32' height='32'><polygon points='16,2 28,28 16,21 4,28' fill='${color}' stroke='#0a0e14' stroke-width='1.5'/></svg>`;
  return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
}

const ICON_NORMAL = aircraftIcon("#4ade80");
const ICON_GROUND = aircraftIcon("#5b6b80");
const ICON_EMERGENCY = aircraftIcon("#ff4757");

interface Props {
  states: LiveState[];
}

export function LiveMap({ states }: Props) {
  const [hovered, setHovered] = useState<LiveState | null>(null);

  const layer = useMemo(
    () =>
      new IconLayer<LiveState>({
        id: "aircraft",
        data: states,
        getPosition: (d) => [d.lon, d.lat],
        getAngle: (d) => 360 - (d.true_track ?? 0),
        getIcon: (d) => {
          const url = EMERGENCY_SQUAWKS.has(d.squawk ?? "")
            ? ICON_EMERGENCY
            : d.on_ground
              ? ICON_GROUND
              : ICON_NORMAL;
          return { url, width: 32, height: 32, anchorY: 20 };
        },
        getSize: 22,
        pickable: true,
        onHover: (info) => setHovered((info.object as LiveState) ?? null),
        updateTriggers: {
          getIcon: [states],
        },
      }),
    [states],
  );

  return (
    <div className="relative h-full w-full">
      <DeckGL initialViewState={INITIAL_VIEW_STATE} controller layers={[layer]} style={{ position: "absolute" }}>
        <div className="h-full w-full [filter:invert(1)_hue-rotate(180deg)_brightness(0.85)_contrast(0.9)_saturate(0.6)]">
          <MapLibreMap mapStyle="https://demotiles.maplibre.org/style.json" />
        </div>
      </DeckGL>
      <RadarSweep />

      {hovered && (
        <div className="pointer-events-none absolute bottom-4 left-4 min-w-56 rounded border border-hairline bg-panel/95 p-3 font-mono text-xs shadow-lg">
          <div className="mb-1 text-sm font-semibold text-phosphor">{hovered.callsign ?? hovered.icao24}</div>
          <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 text-text-dim">
            <span>ICAO24</span>
            <span className="text-text">{hovered.icao24}</span>
            <span>ALT</span>
            <span className="text-text">
              {hovered.baro_altitude_m != null ? `${Math.round(hovered.baro_altitude_m)} m` : "--"}
            </span>
            <span>SPD</span>
            <span className="text-text">
              {hovered.velocity_ms != null ? `${Math.round(hovered.velocity_ms)} m/s` : "--"}
            </span>
            <span>HDG</span>
            <span className="text-text">{hovered.true_track != null ? `${Math.round(hovered.true_track)}°` : "--"}</span>
            <span>SQUAWK</span>
            <span className={EMERGENCY_SQUAWKS.has(hovered.squawk ?? "") ? "text-critical" : "text-text"}>
              {hovered.squawk ?? "--"}
            </span>
          </div>
        </div>
      )}

      <div className="pointer-events-none absolute right-4 top-4 rounded border border-hairline bg-panel/90 px-3 py-1.5 font-mono text-xs text-text-dim">
        AIRCRAFT TRACKED: <span className="text-phosphor">{states.length}</span>
      </div>
    </div>
  );
}

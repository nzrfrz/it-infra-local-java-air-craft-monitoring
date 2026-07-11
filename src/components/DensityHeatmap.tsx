import { MapboxOverlay } from "@deck.gl/mapbox";
import type { Layer } from "@deck.gl/core";
import { PolygonLayer } from "@deck.gl/layers";
import { useMemo } from "react";
import { Map as MapLibreMap, useControl } from "react-map-gl/maplibre";
import type { DensityCell } from "../types";

const INITIAL_VIEW_STATE = {
  longitude: 109.5,
  latitude: -6.8,
  zoom: 4.6,
  pitch: 0,
  bearing: 0,
};

// Matches the legend gradient: low density -> green, high density -> red.
const COLOR_STOPS: [number, number, number, number][] = [
  [47, 138, 88, 140],
  [74, 222, 128, 190],
  [255, 176, 32, 220],
  [255, 71, 87, 255],
];

function densityColor(t: number): [number, number, number, number] {
  const clamped = Math.max(0, Math.min(1, t));
  const segment = clamped * (COLOR_STOPS.length - 1);
  const i = Math.min(COLOR_STOPS.length - 2, Math.floor(segment));
  const localT = segment - i;
  const a = COLOR_STOPS[i];
  const b = COLOR_STOPS[i + 1];
  return [
    a[0] + (b[0] - a[0]) * localT,
    a[1] + (b[1] - a[1]) * localT,
    a[2] + (b[2] - a[2]) * localT,
    a[3] + (b[3] - a[3]) * localT,
  ];
}

function cellPolygon(d: DensityCell): [number, number][] {
  const { lon, lat } = d;
  return [
    [lon, lat],
    [lon + 1, lat],
    [lon + 1, lat + 1],
    [lon, lat + 1],
    [lon, lat],
  ];
}

function DeckOverlay({ layers }: { layers: Layer[] }) {
  const overlay = useControl<MapboxOverlay>(() => new MapboxOverlay({ interleaved: true, layers }));
  overlay.setProps({ layers });
  return null;
}

export function DensityHeatmap({ cells }: { cells: DensityCell[] }) {
  const maxCount = useMemo(() => cells.reduce((max, c) => Math.max(max, c.count), 0), [cells]);

  const layer = useMemo(
    () =>
      new PolygonLayer<DensityCell>({
        id: "density",
        data: cells,
        getPolygon: cellPolygon,
        getFillColor: (d) => densityColor(maxCount > 0 ? d.count / maxCount : 0),
        stroked: false,
        filled: true,
        pickable: false,
      }),
    [cells, maxCount],
  );

  const totalAircraft = useMemo(() => cells.reduce((sum, c) => sum + c.count, 0), [cells]);

  return (
    <div className="relative h-full w-full">
      <div className="h-full w-full [filter:invert(1)_hue-rotate(180deg)_brightness(0.85)_contrast(0.9)_saturate(0.6)]">
        <MapLibreMap initialViewState={INITIAL_VIEW_STATE} mapStyle="https://demotiles.maplibre.org/style.json">
          <DeckOverlay layers={[layer]} />
        </MapLibreMap>
      </div>

      <div className="pointer-events-none absolute right-3 top-3 rounded border border-hairline bg-panel/90 px-3 py-2 font-mono text-[10px] text-text-dim backdrop-blur-sm">
        <div className="mb-1 tracking-widest">KEPADATAN (PESAWAT/GRID)</div>
        <div
          className="h-2 w-40 rounded-sm"
          style={{
            background: "linear-gradient(to right, rgba(47,138,88,0.55), rgba(74,222,128,0.7), rgba(255,176,32,0.85), rgba(255,71,87,1))",
          }}
        />
        <div className="mt-1 flex justify-between">
          <span>Rendah</span>
          <span>Tinggi</span>
        </div>
        <div className="mt-2 border-t border-hairline pt-1 text-text-dim">
          Total pesawat terdeteksi: <span className="text-phosphor">{totalAircraft.toLocaleString("id-ID")}</span>
        </div>
      </div>
    </div>
  );
}

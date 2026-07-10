import { DeckGL } from "@deck.gl/react";
import { HeatmapLayer } from "@deck.gl/aggregation-layers";
import { useMemo } from "react";
import { Map as MapLibreMap } from "react-map-gl/maplibre";
import type { DensityCell } from "../types";

const INITIAL_VIEW_STATE = {
  longitude: 109.5,
  latitude: -6.8,
  zoom: 4.6,
  pitch: 0,
  bearing: 0,
};

export function DensityHeatmap({ cells }: { cells: DensityCell[] }) {
  const layer = useMemo(
    () =>
      new HeatmapLayer<DensityCell>({
        id: "density",
        data: cells,
        getPosition: (d) => [d.lon + 0.5, d.lat + 0.5],
        getWeight: (d) => d.count,
        radiusPixels: 45,
        colorRange: [
          [17, 24, 32, 0],
          [47, 138, 88, 120],
          [74, 222, 128, 170],
          [255, 176, 32, 210],
          [255, 71, 87, 255],
        ],
      }),
    [cells],
  );

  return (
    <div className="relative h-full w-full">
      <DeckGL initialViewState={INITIAL_VIEW_STATE} controller layers={[layer]} style={{ position: "absolute" }}>
        <div className="h-full w-full [filter:invert(1)_hue-rotate(180deg)_brightness(0.85)_contrast(0.9)_saturate(0.6)]">
          <MapLibreMap mapStyle="https://demotiles.maplibre.org/style.json" />
        </div>
      </DeckGL>
    </div>
  );
}

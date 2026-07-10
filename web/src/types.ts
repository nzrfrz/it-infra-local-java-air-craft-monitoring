// Tipe sesuai kontrak data pipeline (docs/design/CONTRACTS.md, C2 & C3).
// Tidak boleh diubah sepihak -- lihat kontrak sebelum menambah field.

export interface LiveState {
  icao24: string;
  callsign: string | null;
  origin_country: string | null;
  lat: number;
  lon: number;
  baro_altitude_m: number | null;
  velocity_ms: number | null;
  true_track: number | null;
  on_ground: boolean | null;
  squawk: string | null;
  ts: number;
  updated_at?: string;
}

export interface ZoneStat {
  _id: string;
  zone: string;
  window_start: number;
  window_end: number;
  aircraft_count: number;
  avg_velocity_ms: number;
  updated_at?: string;
}

export interface Alert {
  _id?: string;
  type: "emergency_squawk" | "density_spike";
  icao24: string | null;
  zone: string | null;
  ts: number;
  severity: "critical" | "warning";
  details: string;
  created_at?: string;
}

export type WsMessage =
  | { channel: "snapshot"; data: { states: LiveState[] } }
  | { channel: "live_states"; data: LiveState }
  | { channel: "zone_stats"; data: ZoneStat }
  | { channel: "alerts"; data: Alert };

export interface DailySummary {
  _id: string;
  total_records: number;
  unique_aircraft: number;
  busiest_hour: number | null;
  top_airlines: { prefix: string; count: number }[];
  generated_at?: string;
}

export interface HourlyBucket {
  hour: number;
  aircraft_count: number;
  avg_velocity_ms: number | null;
}

export interface HourlyResponse {
  date: string;
  hours: HourlyBucket[];
}

export interface DensityCell {
  zone: string;
  lat: number;
  lon: number;
  count: number;
}

export interface DensityResponse {
  date: string;
  cells: DensityCell[];
}

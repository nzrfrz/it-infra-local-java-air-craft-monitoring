import type { DailySummary, DensityResponse, HourlyResponse, LiveState } from "../types";

export const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";
export const WS_URL = API_BASE.replace(/^http/, "ws") + "/ws/live";

class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new ApiError(res.status, body.detail ?? res.statusText);
  }
  return res.json();
}

async function postJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { method: "POST" });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new ApiError(res.status, body.detail ?? res.statusText);
  }
  return res.json();
}

export { ApiError };

export function fetchSummary(date: string) {
  return getJson<DailySummary>(`/api/history/summary?date=${date}`);
}

export function fetchHourly(date: string) {
  return getJson<HourlyResponse>(`/api/history/hourly?date=${date}`);
}

export function fetchDensity(date: string) {
  return getJson<DensityResponse>(`/api/history/density?date=${date}`);
}

export function refreshHistory(date: string) {
  return postJson<{ date: string; status: string }>(`/api/history/refresh?date=${date}`);
}

export function fetchAvailableDates() {
  return getJson<{ dates: string[] }>(`/api/history/available-dates`);
}

export function fetchLiveStates() {
  return getJson<{ states: LiveState[] }>(`/api/live/states`);
}

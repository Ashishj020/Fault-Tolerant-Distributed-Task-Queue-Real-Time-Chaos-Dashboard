import type { Cluster, ExperimentSummary, Job } from "./types";

const API = "/api";

async function parse<T>(res: Response): Promise<T> {
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`);
  }
  return (await res.json()) as T;
}

export const api = {
  cluster: () => fetch(`${API}/cluster`).then((r) => parse<Cluster>(r)),
  jobs: () => fetch(`${API}/jobs`).then((r) => parse<Job[]>(r)),
  job: (id: string) => fetch(`${API}/jobs/${id}`).then((r) => parse<Job>(r)),
  incidents: () => fetch(`${API}/incidents`).then((r) => parse<Record<string, unknown>[]>(r)),
  experiment: () => fetch(`${API}/experiment`).then((r) => parse<ExperimentSummary>(r)),
  submit: (type: string, payload: Record<string, unknown>, idempotency_key?: string) =>
    fetch(`${API}/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ type, payload, idempotency_key }),
    }).then((r) => parse<{ job_id: string; status: string }>(r)),
  batch: (count: number) =>
    fetch(`${API}/jobs/batch`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count }),
    }).then((r) => parse<{ submitted: number }>(r)),
  startChaos: (duration: number, kill_interval: number, workers: string) =>
    fetch(`${API}/chaos/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ duration, kill_interval, workers }),
    }).then((r) => parse<Record<string, unknown>>(r)),
  stopChaos: () =>
    fetch(`${API}/chaos/stop`, { method: "POST" }).then((r) => parse<Record<string, unknown>>(r)),
};

export function wsUrl(): string {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${location.host}/api/ws`;
}

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, wsUrl } from "./api";
import type { Cluster, ExperimentSummary, Job, LiveEvent, WorkerInfo } from "./types";

const emptyCluster: Cluster = {
  health: "operational",
  workers: [],
  expected_workers: ["worker-1", "worker-2", "worker-3"],
  queued_jobs: 0,
  processing: 0,
  completed: 0,
  failed: 0,
  recovery_rate: 100,
  rabbitmq: "disconnected",
  redis: "disconnected",
  chaos: null,
  metrics: {},
  timestamp: new Date().toISOString(),
};

export function useLiveState() {
  const [cluster, setCluster] = useState<Cluster>(emptyCluster);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [events, setEvents] = useState<LiveEvent[]>([]);
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [connected, setConnected] = useState(false);
  const [summary, setSummary] = useState<ExperimentSummary | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const jobsRef = useRef<Job[]>([]);
  const clusterRef = useRef<Cluster>(emptyCluster);

  useEffect(() => {
    jobsRef.current = jobs;
  }, [jobs]);
  useEffect(() => {
    clusterRef.current = cluster;
  }, [cluster]);

  const refresh = useCallback(async () => {
    try {
      const [c, j] = await Promise.all([api.cluster(), api.jobs()]);
      setCluster(c);
      setJobs(j);
      if (c.chaos?.summary) setSummary(c.chaos.summary);
    } catch {
      /* dashboard still renders last snapshot */
    }
  }, []);

  useEffect(() => {
    void refresh();
    const poll = window.setInterval(() => void refresh(), 2000);
    return () => window.clearInterval(poll);
  }, [refresh]);

  useEffect(() => {
    let closed = false;
    let ws: WebSocket | null = null;
    let retry: number | undefined;

    const ingest = (event: LiveEvent) => {
      if (event.event === "snapshot" && event.cluster) {
        setCluster(event.cluster);
        return;
      }
      if (event.event === "worker_heartbeat") {
        setCluster((prev) => {
          const workers = prev.workers.map((w) =>
            w.worker_id === event.worker_id
              ? {
                  ...w,
                  status: String(event.status || w.status),
                  current_job: (event.current_job as string | null) ?? w.current_job,
                  alive: true,
                }
              : w
          );
          return { ...prev, workers };
        });
        return;
      }
      setEvents((prev) => [event, ...prev].slice(0, 250));
      if (event.event === "failover_completed" && event.summary) {
        setSummary(event.summary);
      }
      if (event.job_id) {
        void api.job(event.job_id).then((job) => {
          setJobs((prev) => {
            const idx = prev.findIndex((item) => item.job_id === job.job_id);
            if (idx === -1) return [job, ...prev];
            const next = prev.slice();
            next[idx] = job;
            return next;
          });
        }).catch(() => undefined);
      }
      if (event.event === "WORKER_KILLED" || event.event === "worker_killed") {
        setCluster((prev) => ({
          ...prev,
          health: "degraded",
          workers: prev.workers.map((w) =>
            w.worker_id === event.worker_id ? { ...w, status: "DEAD", alive: false, current_job: null } : w
          ),
        }));
        setToast(`WORKER KILLED  ${event.worker_id}`);
      }
      if (event.event === "worker_started") {
        setCluster((prev) => {
          const exists = prev.workers.some((w) => w.worker_id === event.worker_id);
          const workers: WorkerInfo[] = exists
            ? prev.workers.map((w) =>
                w.worker_id === event.worker_id
                  ? { ...w, status: String(event.status || "RECOVERING"), alive: true }
                  : w
              )
            : [
                ...prev.workers,
                {
                  worker_id: String(event.worker_id),
                  status: String(event.status || "STARTING"),
                  alive: true,
                },
              ];
          return { ...prev, workers };
        });
        setToast(`WORKER RECOVERED  ${event.worker_id}`);
      }
    };

    const connect = () => {
      ws = new WebSocket(wsUrl());
      ws.onopen = () => {
        if (!closed) setConnected(true);
      };
      ws.onclose = () => {
        setConnected(false);
        if (!closed) retry = window.setTimeout(connect, 1200);
      };
      ws.onerror = () => ws?.close();
      ws.onmessage = (msg) => {
        try {
          ingest(JSON.parse(msg.data) as LiveEvent);
        } catch {
          /* ignore malformed frames */
        }
      };
    };
    connect();
    return () => {
      closed = true;
      if (retry) window.clearTimeout(retry);
      ws?.close();
    };
  }, []);

  useEffect(() => {
    if (!toast) return;
    const id = window.setTimeout(() => setToast(null), 3200);
    return () => window.clearTimeout(id);
  }, [toast]);

  const selectedJob = useMemo(
    () => jobs.find((job) => job.job_id === selectedJobId) || null,
    [jobs, selectedJobId]
  );

  return {
    cluster,
    jobs,
    events,
    selectedJob,
    selectedJobId,
    setSelectedJobId,
    connected,
    summary,
    setSummary,
    toast,
    refresh,
  };
}

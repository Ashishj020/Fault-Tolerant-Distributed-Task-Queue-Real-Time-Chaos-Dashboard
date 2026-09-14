import { useState } from "react";
import { api } from "../api";
import type { Cluster, Job } from "../types";

export function ChaosPanel({
  cluster,
  jobs,
  onSubmitted,
  onSelectJob,
}: {
  cluster: Cluster;
  jobs: Job[];
  onSubmitted: () => void;
  onSelectJob: (id: string) => void;
}) {
  const [duration, setDuration] = useState(120);
  const [killInterval, setKillInterval] = useState(15);
  const [target, setTarget] = useState("random");
  const chaos = cluster.chaos;
  const running = chaos?.status === "running";
  const alive = cluster.workers.filter((w) => w.alive && w.status !== "DEAD").length;

  return (
    <div className="side panel">
      <div className="section-h">
        CHAOS ENGINE
        <span>{running ? "ACTIVE" : "READY"}</span>
      </div>
      <div className="chaos">
        <label>
          Duration (s)
          <input type="number" value={duration} onChange={(e) => setDuration(Number(e.target.value))} />
        </label>
        <label>
          Kill interval (s)
          <input type="number" value={killInterval} onChange={(e) => setKillInterval(Number(e.target.value))} />
        </label>
        <label>
          Workers
          <select value={target} onChange={(e) => setTarget(e.target.value)}>
            <option value="random">Random</option>
            <option value="worker-1">worker-1</option>
            <option value="worker-2">worker-2</option>
            <option value="worker-3">worker-3</option>
          </select>
        </label>
        <div className="row">
          {running ? (
            <button className="danger" onClick={() => void api.stopChaos()}>
              STOP CHAOS
            </button>
          ) : (
            <button className="danger" onClick={() => void api.startChaos(duration, killInterval, target)}>
              START CHAOS TEST
            </button>
          )}
        </div>
        <div className="row">
          <button className="ghost" onClick={() => void api.batch(12).then(onSubmitted)}>
            SUBMIT 12 JOBS
          </button>
        </div>
        {running && (
          <div className="metric-mini">
            <div>
              <b>
                {alive}/{cluster.expected_workers.length || 3}
              </b>
              workers
            </div>
            <div>
              <b>{chaos?.workers_killed ?? 0}</b>
              failures
            </div>
            <div>
              <b>{cluster.metrics.jobs_recovered_total ?? 0}</b>
              recovered
            </div>
            <div>
              <b>{chaos?.jobs_inflight_during_kill ?? 0}</b>
              in-flight
            </div>
          </div>
        )}
        <div className="metric-mini">
          <div>
            <b>{cluster.metrics.duplicate_deliveries ?? cluster.metrics.jobs_redelivered_total ?? 0}</b>
            dup deliveries
          </div>
          <div>
            <b>{cluster.metrics.duplicate_side_effects ?? 0}</b>
            dup side effects
          </div>
        </div>
      </div>
      <div className="section-h">JOBS</div>
      <div className="jobs-mini">
        {jobs.slice(0, 40).map((job) => (
          <div key={job.job_id} className="job-row" onClick={() => onSelectJob(job.job_id)}>
            <span>{job.job_id}</span>
            <span>{job.type.replace("_", " ")}</span>
            <span className={`badge ${job.status}`}>{job.status}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

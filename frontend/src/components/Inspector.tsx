import type { Job } from "../types";

export function Inspector({ job }: { job: Job | null }) {
  if (!job) {
    return (
      <div className="panel inspector">
        <div className="section-h">JOB INSPECTOR</div>
        <div style={{ color: "var(--muted)" }}>Select a job particle or row.</div>
      </div>
    );
  }
  return (
    <div className="panel inspector">
      <div className="section-h">JOB INSPECTOR</div>
      <div className="kv">
        <span>JOB ID</span>
        <b>{job.job_id}</b>
        <span>TYPE</span>
        <b>{job.type}</b>
        <span>STATUS</span>
        <b>{job.recovered_at ? "RECOVERED" : job.status.toUpperCase()}</b>
        <span>ATTEMPTS</span>
        <b>{job.attempt}</b>
        <span>IDEMPOTENCY</span>
        <b>{job.idempotency_key}</b>
        <span>FIRST WORKER</span>
        <b>{job.first_worker || "—"}</b>
        <span>RECOVERY WORKER</span>
        <b>{job.recovery_worker || "—"}</b>
        <span>FAILURE</span>
        <b>{job.failure_reason || "—"}</b>
        <span>RECOVERY TIME</span>
        <b>{job.recovery_ms != null ? `${(job.recovery_ms / 1000).toFixed(2)}s` : "—"}</b>
        <span>SIDE EFFECT</span>
        <b>{job.skipped_duplicate ? "SKIPPED DUPLICATE" : job.side_effect_committed ? "COMMITTED ONCE" : "PENDING"}</b>
      </div>
      {job.timeline.slice(-6).map((entry, idx) => (
        <div key={idx} style={{ opacity: 0.8, marginBottom: 4 }}>
          {String(entry.timestamp || "").slice(11, 19)}  {String(entry.event)}
        </div>
      ))}
    </div>
  );
}

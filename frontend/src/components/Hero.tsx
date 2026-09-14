import type { Cluster } from "../types";

export function Hero({ cluster }: { cluster: Cluster }) {
  const workers = cluster.expected_workers.length || 3;
  const alive = cluster.workers.filter((w) => w.alive && w.status !== "DEAD").length;
  const health = cluster.health || "operational";
  return (
    <header className="hero panel">
      <div>
        <div className="kicker">DISTRIBUTED TASK QUEUE</div>
        <h1 className="title">CLUSTER CONTROL</h1>
      </div>
      <div className="health">
        <div className="health-label">CLUSTER HEALTH</div>
        <div className={`health-value ${health}`}>
          <span className="led" />
          {health === "operational" ? "OPERATIONAL" : health === "degraded" ? "DEGRADED" : "CRITICAL"}
        </div>
      </div>
      <div className="stats">
        <div className="stat">
          <b>
            {alive} / {workers}
          </b>
          <span>WORKERS</span>
        </div>
        <div className="stat">
          <b>{cluster.queued_jobs}</b>
          <span>QUEUED</span>
        </div>
        <div className="stat">
          <b>{cluster.processing}</b>
          <span>PROCESSING</span>
        </div>
        <div className="stat">
          <b>{cluster.completed.toLocaleString()}</b>
          <span>COMPLETED</span>
        </div>
        <div className="stat">
          <b>{(cluster.recovery_rate ?? 100).toFixed(2)}%</b>
          <span>RECOVERY</span>
        </div>
      </div>
    </header>
  );
}

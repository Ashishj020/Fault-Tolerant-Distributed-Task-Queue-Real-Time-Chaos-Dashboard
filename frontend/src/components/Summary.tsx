import type { ExperimentSummary } from "../types";

export function Summary({
  summary,
  onClose,
}: {
  summary: ExperimentSummary;
  onClose: () => void;
}) {
  const rows: Array<[string, string | number]> = [
    ["Duration", `${summary.duration}.0s`],
    ["Workers killed", summary.workers_killed],
    ["Jobs submitted", summary.jobs_submitted],
    ["Jobs in-flight during kill", summary.jobs_inflight_during_kill],
    ["Jobs recovered", summary.jobs_recovered],
    ["Jobs lost", summary.jobs_lost],
    ["Dead-lettered", summary.dead_lettered],
    ["Retry count", summary.retry_count],
    ["Duplicate deliveries", summary.duplicate_deliveries],
    ["Duplicate side effects", summary.duplicate_side_effects],
    ["Median recovery time", `${summary.median_recovery_s.toFixed(2)}s`],
    ["P95 recovery time", `${summary.p95_recovery_s.toFixed(2)}s`],
    ["Recovery success rate", `${summary.recovery_success_rate}%`],
  ];
  return (
    <div className="summary" onClick={onClose}>
      <div className="summary-card panel" onClick={(e) => e.stopPropagation()}>
        <h2>CHAOS TEST COMPLETE</h2>
        <div className="grid-metrics">
          {rows.map(([k, v]) => (
            <div key={k}>
              <span>{k}</span>
              <b className={k === "Jobs lost" && v === 0 ? "lost-zero" : ""}>{v}</b>
            </div>
          ))}
        </div>
        <div className="row" style={{ marginTop: 16 }}>
          <button onClick={onClose}>CLOSE</button>
        </div>
      </div>
    </div>
  );
}

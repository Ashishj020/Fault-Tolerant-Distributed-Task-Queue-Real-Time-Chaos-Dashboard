import { useEffect, useRef } from "react";
import type { LiveEvent } from "../types";

function severity(event: string): "ok" | "warn" | "bad" | "info" {
  const u = event.toLowerCase();
  if (u.includes("kill") || u.includes("dead") || u.includes("fail")) return "bad";
  if (u.includes("retry") || u.includes("requeue") || u.includes("degraded")) return "warn";
  if (u.includes("recover") || u.includes("complete") || u.includes("success")) return "ok";
  return "info";
}

function clock(ts?: string): string {
  if (!ts) return "--:--:--";
  const d = new Date(ts);
  return d.toLocaleTimeString("en-GB", { hour12: false });
}

export function Terminal({ events }: { events: LiveEvent[] }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (ref.current) ref.current.scrollTop = 0;
  }, [events[0]?.timestamp]);
  const filtered = events.filter((e) => e.event !== "worker_heartbeat" && e.event !== "snapshot");
  return (
    <div className="right panel" style={{ minHeight: 0 }}>
      <div className="section-h">FAILOVER EVENTS</div>
      <div className="terminal" ref={ref}>
        {filtered.slice(0, 80).map((event, idx) => (
          <div className="term-line" key={`${event.timestamp}-${event.event}-${idx}`}>
            <span>{clock(event.timestamp)}</span>
            <span className={`sev ${severity(event.event)}`} />
            <span>
              <b>{event.event}</b>
              {event.worker_id ? `  ${event.worker_id}` : ""}
              {event.job_id ? `  ${event.job_id}` : ""}
              {event.recovery_ms != null ? `  recovery=${(event.recovery_ms / 1000).toFixed(2)}s` : ""}
              {event.error ? `  ${event.error}` : ""}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

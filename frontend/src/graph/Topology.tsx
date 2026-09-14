import { useEffect, useRef } from "react";
import type { Cluster, Job, LiveEvent } from "../types";

type Node = {
  id: string;
  label: string;
  kind: "infra" | "worker";
  restX: number;
  restY: number;
  x: number;
  y: number;
  vx: number;
  vy: number;
  r: number;
  flash: number;
  collapse: number;
  shock: number;
  recovered: number;
};

type Particle = {
  id: string;
  jobId: string;
  from: string;
  to: string;
  t: number;
  speed: number;
  kind: string;
  label: string;
};

const REST: Record<string, [number, number]> = {
  producer: [0.5, 0.12],
  rabbitmq: [0.5, 0.38],
  "worker-1": [0.18, 0.64],
  "worker-2": [0.5, 0.72],
  "worker-3": [0.82, 0.64],
  redis: [0.32, 0.91],
  dlq: [0.72, 0.91],
};

function colorFor(kind: string, dead = false): string {
  if (dead) return "#ff4d6d";
  switch (kind) {
    case "queued":
      return "#5dffc6";
    case "processing":
      return "#3ee0ff";
    case "retry":
      return "#ffbf3c";
    case "recovery":
      return "#ff8a5b";
    case "dead":
      return "#ff4d6d";
    case "success":
      return "#8b7bff";
    default:
      return "#3ee0ff";
  }
}

function workerStatus(cluster: Cluster, id: string) {
  const w = cluster.workers.find((item) => item.worker_id === id);
  if (!w) return { status: "DEAD", alive: false, job: null as string | null };
  const alive = Boolean(w.alive) && w.status !== "DEAD";
  return { status: alive ? w.status : "DEAD", alive, job: w.current_job || null };
}

export function Topology({
  cluster,
  jobs,
  events,
  onSelectJob,
}: {
  cluster: Cluster;
  jobs: Job[];
  events: LiveEvent[];
  onSelectJob: (id: string) => void;
}) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const nodesRef = useRef<Node[]>([]);
  const partsRef = useRef<Particle[]>([]);
  const seenRef = useRef<Set<string>>(new Set());
  const hoverRef = useRef<string | null>(null);

  useEffect(() => {
    nodesRef.current = Object.entries(REST).map(([id, [x, y]]) => ({
      id,
      label: id === "rabbitmq" ? "RABBITMQ" : id === "producer" ? "PRODUCER" : id === "redis" ? "RESULT STORE" : id === "dlq" ? "DEAD LETTER" : id.toUpperCase(),
      kind: id.startsWith("worker") ? "worker" : "infra",
      restX: x,
      restY: y,
      x,
      y,
      vx: 0,
      vy: 0,
      r: id.startsWith("worker") ? 28 : 32,
      flash: 0,
      collapse: 1,
      shock: 0,
      recovered: 0,
    }));
  }, []);

  useEffect(() => {
    const newest = events[0];
    if (!newest) return;
    const key = `${newest.timestamp}:${newest.event}:${newest.job_id}:${newest.worker_id}`;
    if (seenRef.current.has(key)) return;
    seenRef.current.add(key);
    const spawn = (from: string, to: string, jobId: string, kind: string) => {
      partsRef.current.push({
        id: `${key}:${from}:${to}`,
        jobId,
        from,
        to,
        t: 0,
        speed: kind === "recovery" ? 0.018 : 0.014,
        kind,
        label: jobId,
      });
    };
    const jobId = newest.job_id || "";
    const worker = newest.worker_id || "";
    switch (newest.event) {
      case "job_queued":
        spawn("producer", "rabbitmq", jobId, "queued");
        break;
      case "job_assigned":
      case "worker_processing":
        spawn("rabbitmq", worker || "worker-1", jobId, newest.event === "job_assigned" && newest.attempt && Number(newest.attempt) > 1 ? "recovery" : "processing");
        break;
      case "job_requeued":
        spawn(worker || "worker-2", "rabbitmq", jobId, "recovery");
        break;
      case "job_retrying":
        spawn(worker || "rabbitmq", "rabbitmq", jobId, "retry");
        break;
      case "job_completed":
        spawn(worker || "rabbitmq", "redis", jobId, "success");
        break;
      case "job_dead_lettered":
        spawn(worker || "rabbitmq", "dlq", jobId, "dead");
        break;
      case "WORKER_KILLED":
      case "worker_killed": {
        const node = nodesRef.current.find((n) => n.id === worker);
        if (node) {
          node.flash = 1;
          node.shock = 1;
          node.collapse = 1;
        }
        (newest.inflight_jobs || []).forEach((id) => spawn(worker, "rabbitmq", id, "recovery"));
        break;
      }
      case "worker_started": {
        const node = nodesRef.current.find((n) => n.id === worker);
        if (node) {
          node.recovered = 1;
          node.collapse = 0.2;
        }
        break;
      }
      default:
        break;
    }
  }, [events]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    let raf = 0;
    const loop = () => {
      const rect = canvas.parentElement?.getBoundingClientRect();
      const w = Math.max(1, Math.floor(rect?.width || 800));
      const h = Math.max(1, Math.floor(rect?.height || 500));
      const dpr = window.devicePixelRatio || 1;
      if (canvas.width !== w * dpr || canvas.height !== h * dpr) {
        canvas.width = w * dpr;
        canvas.height = h * dpr;
        canvas.style.width = `${w}px`;
        canvas.style.height = `${h}px`;
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);

      const nodes = nodesRef.current;
      for (const node of nodes) {
        const info = node.kind === "worker" ? workerStatus(cluster, node.id) : null;
        const dead = Boolean(info && !info.alive);
        const target = dead ? 0.28 : 1;
        node.collapse += (target - node.collapse) * 0.08;
        node.flash *= 0.92;
        node.shock *= 0.96;
        node.recovered *= 0.96;
        const ax = (node.restX - node.x) * 0.05;
        const ay = (node.restY - node.y) * 0.05;
        node.vx = (node.vx + ax) * 0.86;
        node.vy = (node.vy + ay) * 0.86;
        node.x += node.vx + Math.sin(performance.now() / 900 + node.restX * 8) * 0.00025;
        node.y += node.vy;
      }
      for (let i = 0; i < nodes.length; i += 1) {
        for (let j = i + 1; j < nodes.length; j += 1) {
          const a = nodes[i];
          const b = nodes[j];
          const dx = (a.x - b.x) * w;
          const dy = (a.y - b.y) * h;
          const dist = Math.hypot(dx, dy) || 1;
          if (dist < 90) {
            const force = ((90 - dist) / 90) * 0.0004;
            a.vx += (dx / dist) * force;
            a.vy += (dy / dist) * force;
            b.vx -= (dx / dist) * force;
            b.vy -= (dy / dist) * force;
          }
        }
      }

      const pos = (id: string) => {
        const n = nodes.find((item) => item.id === id) || nodes[0];
        return { x: n.x * w, y: n.y * h, node: n };
      };

      const edges: Array<[string, string]> = [
        ["producer", "rabbitmq"],
        ["rabbitmq", "worker-1"],
        ["rabbitmq", "worker-2"],
        ["rabbitmq", "worker-3"],
        ["worker-1", "redis"],
        ["worker-2", "redis"],
        ["worker-3", "redis"],
        ["rabbitmq", "dlq"],
      ];
      ctx.lineWidth = 1.2;
      for (const [from, to] of edges) {
        const a = pos(from);
        const b = pos(to);
        const dead = to.startsWith("worker") && !workerStatus(cluster, to).alive;
        ctx.strokeStyle = dead ? "rgba(255,77,109,0.18)" : "rgba(62,224,255,0.18)";
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        ctx.quadraticCurveTo((a.x + b.x) / 2 + (a.y - b.y) * 0.12, (a.y + b.y) / 2 + (b.x - a.x) * 0.08, b.x, b.y);
        ctx.stroke();
      }

      const next: Particle[] = [];
      for (const p of partsRef.current) {
        p.t += p.speed;
        if (p.t < 1.05) next.push(p);
        const a = pos(p.from);
        const b = pos(p.to);
        const cpx = (a.x + b.x) / 2 + (a.y - b.y) * 0.12;
        const cpy = (a.y + b.y) / 2 + (b.x - a.x) * 0.08;
        const t = Math.min(1, p.t);
        const mt = 1 - t;
        const x = mt * mt * a.x + 2 * mt * t * cpx + t * t * b.x;
        const y = mt * mt * a.y + 2 * mt * t * cpy + t * t * b.y;
        ctx.beginPath();
        ctx.fillStyle = colorFor(p.kind);
        ctx.shadowColor = colorFor(p.kind);
        ctx.shadowBlur = 16;
        ctx.arc(x, y, p.kind === "recovery" ? 5.5 : 4.2, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;
        ctx.fillStyle = "rgba(215,231,244,0.85)";
        ctx.font = "10px IBM Plex Mono";
        ctx.fillText(p.label, x + 8, y - 8);
      }
      partsRef.current = next;

      for (const node of nodes) {
        const info = node.kind === "worker" ? workerStatus(cluster, node.id) : null;
        const dead = Boolean(info && !info.alive);
        const x = node.x * w;
        const y = node.y * h;
        const radius = node.r * node.collapse;
        if (node.shock > 0.04) {
          ctx.beginPath();
          ctx.strokeStyle = `rgba(255,77,109,${node.shock})`;
          ctx.lineWidth = 2;
          ctx.arc(x, y, radius + (1 - node.shock) * 48, 0, Math.PI * 2);
          ctx.stroke();
        }
        const grd = ctx.createRadialGradient(x, y, 4, x, y, radius + 18);
        const hue = dead ? "#ff4d6d" : node.id === "rabbitmq" ? "#3ee0ff" : node.id === "dlq" ? "#ffbf3c" : node.id === "redis" ? "#8b7bff" : "#5dffc6";
        grd.addColorStop(0, `${hue}55`);
        grd.addColorStop(1, "transparent");
        ctx.fillStyle = grd;
        ctx.beginPath();
        ctx.arc(x, y, radius + 18, 0, Math.PI * 2);
        ctx.fill();
        ctx.beginPath();
        ctx.fillStyle = dead ? "rgba(40,8,14,0.9)" : "rgba(8,16,28,0.9)";
        ctx.strokeStyle = node.flash > 0.2 ? "#ff4d6d" : hue;
        ctx.lineWidth = 2;
        ctx.arc(x, y, radius, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();
        ctx.fillStyle = dead ? "#ff4d6d" : "#d7e7f4";
        ctx.font = "11px Rajdhani";
        ctx.textAlign = "center";
        ctx.fillText(node.label, x, y + radius + 14);
        if (node.kind === "worker") {
          ctx.fillStyle = hue;
          ctx.font = "10px IBM Plex Mono";
          ctx.fillText(dead ? "DEAD" : info?.status || "IDLE", x, y + 4);
          if (info?.job) {
            ctx.fillStyle = "#3ee0ff";
            ctx.fillText(info.job, x, y + radius + 26);
          }
        }
        if (node.recovered > 0.2) {
          ctx.fillStyle = `rgba(93,255,198,${node.recovered})`;
          ctx.font = "11px IBM Plex Mono";
          ctx.fillText("WORKER RECOVERED", x, y - radius - 10);
        }
      }

      const processing = jobs.filter((j) => j.status === "processing" || j.status === "assigned").length;
      ctx.textAlign = "left";
      ctx.fillStyle = "rgba(127,147,168,0.9)";
      ctx.font = "11px IBM Plex Mono";
      ctx.fillText(`LIVE EDGES  jobs in flight ${processing}   particles ${partsRef.current.length}`, 16, 22);

      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    const onClick = (ev: MouseEvent) => {
      const rect = canvas.getBoundingClientRect();
      const x = ev.clientX - rect.left;
      const y = ev.clientY - rect.top;
      const w = rect.width;
      const h = rect.height;
      for (const p of partsRef.current) {
        if (p.jobId) onSelectJob(p.jobId);
      }
      for (const node of nodesRef.current) {
        const dx = x - node.x * w;
        const dy = y - node.y * h;
        if (Math.hypot(dx, dy) < node.r + 8 && node.kind === "worker") {
          const info = workerStatus(cluster, node.id);
          if (info.job) onSelectJob(info.job);
        }
      }
    };
    canvas.addEventListener("click", onClick);
    return () => {
      cancelAnimationFrame(raf);
      canvas.removeEventListener("click", onClick);
    };
  }, [cluster, jobs, onSelectJob]);

  return (
    <div className="graph-wrap panel">
      <canvas ref={canvasRef} />
    </div>
  );
}

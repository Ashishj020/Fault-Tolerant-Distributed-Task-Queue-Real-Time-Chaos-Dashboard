export type JobStatus =
  | "queued"
  | "assigned"
  | "processing"
  | "success"
  | "failure"
  | "retry"
  | "dead-letter";

export type WorkerInfo = {
  worker_id: string;
  status: string;
  current_job?: string | null;
  alive?: boolean;
  timestamp?: string;
  started_at?: string;
};

export type Job = {
  job_id: string;
  type: string;
  payload: Record<string, unknown>;
  idempotency_key: string;
  attempt: number;
  status: JobStatus;
  created_at: string;
  updated_at: string;
  assigned_worker?: string | null;
  first_worker?: string | null;
  recovery_worker?: string | null;
  result?: Record<string, unknown> | null;
  error?: string | null;
  failure_reason?: string | null;
  skipped_duplicate?: boolean;
  side_effect_committed?: boolean;
  redelivered?: boolean;
  inflight_failure_at?: string | null;
  recovered_at?: string | null;
  recovery_ms?: number | null;
  timeline: Array<Record<string, unknown>>;
};

export type Cluster = {
  health: "operational" | "degraded" | "critical" | string;
  workers: WorkerInfo[];
  expected_workers: string[];
  queued_jobs: number;
  processing: number;
  completed: number;
  failed: number;
  recovery_rate: number;
  rabbitmq: string;
  redis: string;
  chaos: ChaosState | null;
  metrics: Record<string, number>;
  timestamp: string;
};

export type ChaosState = {
  status: string;
  duration?: number;
  kill_interval?: number;
  workers_killed?: number;
  jobs_inflight_during_kill?: number;
  started_at?: string;
  summary?: ExperimentSummary;
};

export type ExperimentSummary = {
  duration: number;
  workers: number;
  workers_killed: number;
  jobs_submitted: number;
  jobs_inflight_during_kill: number;
  jobs_recovered: number;
  jobs_lost: number;
  dead_lettered: number;
  retry_count: number;
  duplicate_deliveries: number;
  duplicate_side_effects: number;
  median_recovery_s: number;
  p95_recovery_s: number;
  recovery_success_rate: number;
};

export type LiveEvent = {
  event: string;
  timestamp?: string;
  job_id?: string;
  worker_id?: string;
  status?: string;
  attempt?: number;
  job_type?: string;
  recovery_ms?: number;
  inflight_jobs?: string[];
  current_job?: string | null;
  error?: string;
  delay_ms?: number;
  skipped_duplicate?: boolean;
  summary?: ExperimentSummary;
  cluster?: Cluster;
  [key: string]: unknown;
};

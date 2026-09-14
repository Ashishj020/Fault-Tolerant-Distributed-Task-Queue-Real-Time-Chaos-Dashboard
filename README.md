# Fault-Tolerant Distributed Task Queue

A working cluster, not a slideshow: three killable Python workers, RabbitMQ with manual ACKs, Redis-backed idempotency, and a live chaos dashboard.

```bash
cp .env.example .env
docker compose up --build
```

Dashboard: [http://localhost:8081](http://localhost:8081)  
API: [http://localhost:8010](http://localhost:8010)  
RabbitMQ UI: [http://localhost:15672](http://localhost:15672) (guest/guest)

> If 8000/8080 are already bound on the host, this compose file publishes the API on **8010** and the dashboard on **8081**. Inside Docker the services still talk on their default ports.

Then:

```bash
make submit-jobs
make chaos
```

Watch a worker get **SIGKILL**ed mid-job, the unacked message return to `jobs.main`, a surviving worker finish it, and the side-effect store record **exactly one** result.

---

## Architecture

```text
                    ┌─────────────────┐
                    │   Job Producer  │  FastAPI POST /jobs
                    └────────┬────────┘
                             │ persistent publish
                             ▼
                    ┌─────────────────┐
                    │    RabbitMQ     │  jobs.direct → jobs.main
                    │  (durable, QoS) │  retry TTL queues + DLQ
                    └────────┬────────┘
             ┌───────────────┼────────────────┐
             ▼               ▼                ▼
        worker-1         worker-2         worker-3     prefetch=1, manual ACK
             │               │                 │
             └───────────────┼─────────────────┘
                             ▼
                    ┌─────────────────┐
                    │  Redis store    │  job state, locks, side effects,
                    │                 │  heartbeats, metrics, pub/sub
                    └─────────────────┘
                             │
                             ▼
                    WebSocket dashboard (control room)
```

| Component | Role |
| --- | --- |
| **API** (`api`) | Producer, cluster snapshot, chaos controller, WebSocket fan-out |
| **RabbitMQ** | Durable broker: unacked redelivery, retry TTL, dead-letter |
| **Redis** | Job lifecycle, atomic idempotency (`SET NX`), worker heartbeats, event bus |
| **Workers** | Identical image, unique `WORKER_ID`, independently killable containers |
| **Frontend** | Dark-glass control room: topology, particles, terminal, chaos panel |
| **Chaos engine** | `docker kill` on real containers, then `docker start` |

---

## Message delivery semantics (at-least-once)

1. Producer publishes a **persistent** message to the durable `jobs.direct` exchange (`delivery_mode=2`).
2. A worker consumes with **manual acknowledgements** and `prefetch_count=1`.
3. The worker does **not** ACK when it starts. It processes, commits the side effect, *then* ACKs.
4. If the worker is `SIGKILL`ed, the AMQP channel dies with the message still unacked.
5. RabbitMQ detects the lost consumer and makes the message available again (`redelivered=true`).
6. A surviving worker receives it, skips a duplicate side effect if one was already committed, finishes, and ACKs.

That is at-least-once: a job may be delivered more than once; it is never silently dropped because of a worker crash.

### When ACK happens, and why

ACK is issued only when `aio_pika.IncomingMessage.process()` exits **without** an exception — after success **or** after the job has been published onto a retry/DLQ topology. A crash before that point is an implicit NACK from the broker's point of view (unacked → requeue).

We never ACK-then-process. That would lose work on crash.

---

## Retry architecture

Application failures (not crashes) are routed off the main queue so waiting jobs do not block `jobs.main`.

```text
jobs.main  --fail-->  jobs.retry.{2000|4000|8000|16000}
                          │  x-message-ttl
                          ▼
                     jobs.requeue  -->  jobs.main
                          │
                     MAX_RETRIES exceeded
                          ▼
                     jobs.dead  (DLQ)
```

Delay: `base_delay × 2^(attempt-1)`, capped at `MAX_RETRY_DELAY`, plus jitter. Dedicated TTL queues are used because **per-message TTL in a shared FIFO queue is unsafe** (a long-TTL head blocks shorter messages).

Configure with `MAX_RETRIES`, `BASE_RETRY_DELAY`, `MAX_RETRY_DELAY`.

---

## Idempotency

At-least-once means two workers can see the same logical job.

**Not** `GET / if missing / process / SET`. That races.

Mechanism:

1. `GET side_effects:{idempotency_key}` — already committed? skip and ACK.
2. `SET idempotency:lock:{key} NX EX ttl` — atomic claim of the right to run.
3. On crash/redelivery, the lock of a dead owner is stolen; on live contention, the loser waits for the commit.
4. Side effect is recorded with a second `SET NX` on `side_effects:{key}`.
5. Only the first `SET NX` counts as a side effect. Dashboard: **Duplicate deliveries > 0**, **Duplicate side effects = 0**.

Side effects are real files under `/data/results/{idempotency_key}.{png,md,json}` plus the Redis record.

---

## Worker recovery

```text
worker-2 processing job_4921
        │  chaos: docker kill worker-2
        X
NO ACK
RabbitMQ redelivers job_4921
worker-1 claims lock, processes (or skips duplicate side effect)
ACK
job_4921 SUCCESS / RECOVERED
chaos: docker start worker-2
worker-2 STARTING → READY → IDLE
```

Recovery time = `job_recovered_timestamp − worker_failure_timestamp` for jobs that were in-flight on the killed worker.

---

## Chaos testing

From the dashboard: **START CHAOS TEST**.

From the host (API must be up):

```bash
python chaos/chaos.py --duration 120 --kill-interval 15 --workers random --submit 12
```

or:

```bash
make chaos
```

The engine uses the Docker socket (`docker kill`, then `docker start`). It does **not** flip a "dead" bit in Redis and call that failure.

---

## Observability

**WebSocket** `/ws` (proxied as `/api/ws`) streams real backend events:

`job_queued`, `job_assigned`, `worker_processing`, `job_retrying`, `job_requeued`, `job_recovered`, `job_dead_lettered`, `WORKER_KILLED`, `worker_started`, `failover_started`, `failover_completed`, heartbeats.

**HTTP**

| Endpoint | Purpose |
| --- | --- |
| `POST /jobs` | Enqueue |
| `GET /jobs/{id}` | Inspector payload |
| `GET /cluster` | Health, workers, queue depth |
| `GET /incidents` | Failover log |
| `GET /metrics` | JSON counters |
| `GET /metrics/prometheus` | Prometheus text |
| `POST /chaos/start` | Real container kills |

Counters include `jobs_submitted_total`, `jobs_completed_total`, `jobs_redelivered_total`, `jobs_duplicate_total`, `job_recovery_duration_seconds`, `worker_failures_total`, `active_workers`, `queued_jobs`, `processing_jobs`.

Logs are structured JSON (`timestamp`, `level`, `event`, `worker_id`, `job_id`, `attempt`).

---

## Make targets

```bash
make up            # docker compose up --build
make down
make logs
make test          # pytest inside the API container
make submit-jobs
make chaos
make reset
```

---

## Tests

```bash
docker compose up -d --build
make test
```

| File | Invariant |
| --- | --- |
| `test_delivery.py` | Job is queued, assigned, completed |
| `test_retry.py` | Backoff formula + `fail_until_attempt` succeeds |
| `test_idempotency.py` | Same key → one job; `SET NX` race → one winner; one side effect |
| `test_dead_letter.py` | `force_fail` exceeds retries → DLQ |
| `test_failover.py` | Kill the processing worker → job still succeeds |

---

## Chaos experiment report

Recorded on 2026-09-14 against this stack (`docker compose up --build`, then a 40s live kill/recover run from the dashboard). Failures were real `docker kill`s. Values below are from `/experiment`, not placeholders.

| Metric | Result |
| --- | ---: |
| Test Duration | 40.0s |
| Workers | 3 |
| Workers Killed | 2 |
| Jobs Submitted | 18 |
| Jobs In-Flight During Failures | 2 |
| Jobs Recovered | 2 |
| Jobs Lost | **0** |
| Retry Count | 0 |
| Duplicate Deliveries | 2 |
| Duplicate Side Effects | **0** |
| Median Recovery Time | 9.804s |
| P95 Recovery Time | 11.17s |
| Recovery success rate | 100% |

Example recovered job `job_e0e56939a8`: first worker `worker-3` was killed mid-job → RabbitMQ redelivered → `worker-2` completed it in 11.17s. Side-effect store recorded one result.

Invariant: **Jobs Lost = 0**. Duplicate side effects = 0 even when duplicate deliveries > 0.

Re-run with `make chaos` (120s) to replace this table with a longer experiment.

---

## Screenshots / recording

Live captures from this repo's running stack (http://localhost:8081):

- Control room with 3 idle workers, RabbitMQ, and completed jobs
- Chaos active: job particles on the edges, `worker-3` flashing **WORKER RECOVERED**, in-flight job `job_e0e56939a8` rerouted
- **CHAOS TEST COMPLETE** overlay with measured totals (jobs lost = 0, duplicate side effects = 0)

Re-record a GIF/video with any screen recorder while `make chaos` runs. The kill is a real container SIGKILL (`docker kill worker-*`), not a CSS-only animation.

---

## Layout

```text
backend/     FastAPI, workers, broker topology, idempotency
frontend/    Vite + React control room
chaos/       CLI client (talks to the API; API talks to Docker)
tests/       delivery, retry, idempotency, DLQ, failover
```

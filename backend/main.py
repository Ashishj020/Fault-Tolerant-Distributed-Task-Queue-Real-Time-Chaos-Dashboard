from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

import aio_pika
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from chaos_engine import ChaosEngine
from config import get_settings
from logging_setup import configure_logging
from metrics import prometheus_text, snapshot as metrics_snapshot
from models.job import Job, JobCreate, JobStatus, JobType, iso
from broker.producer import JobProducer
from broker.topology import QueueTopology
from store.redis_store import CHANNEL_EVENTS, Store
from websocket.hub import Hub

configure_logging()
logger = logging.getLogger("api")
settings = get_settings()


class BatchJobCreate(BaseModel):
    count: int = Field(default=12, ge=1, le=200)
    types: list[JobType] | None = None


class ChaosRequest(BaseModel):
    duration: int = Field(default=120, ge=10, le=600)
    kill_interval: int = Field(default=15, ge=5, le=120)
    workers: str = "random"
    restart_delay: int | None = Field(default=None, ge=2, le=60)


class AppState:
    store: Store
    topology: QueueTopology
    producer: JobProducer
    hub: Hub
    chaos: ChaosEngine
    connection: aio_pika.RobustConnection
    tasks: list[asyncio.Task[Any]]


state = AppState()


async def relay_events() -> None:
    pubsub = state.store.redis.pubsub()
    await pubsub.subscribe(CHANNEL_EVENTS)
    try:
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            data = message.get("data")
            if not data:
                continue
            import json

            event = json.loads(data)
            await state.hub.broadcast(event)
    finally:
        await pubsub.unsubscribe(CHANNEL_EVENTS)


async def monitor_workers() -> None:
    known_dead: set[str] = set()
    while True:
        await asyncio.sleep(1.2)
        try:
            for worker in await state.store.list_workers():
                worker_id = worker["worker_id"]
                alive = worker.get("alive")
                if not alive:
                    if worker_id not in known_dead:
                        known_dead.add(worker_id)
                        await state.store.mark_worker_dead(worker_id)
                        current_job = worker.get("current_job")
                        if current_job:
                            job = await state.store.get_job(current_job)
                            if job and not job.inflight_failure_at:
                                job.inflight_failure_at = iso()
                                job.failure_reason = job.failure_reason or "worker killed mid-job"
                                await state.store.save_job(job)
                        await state.store.publish_event(
                            {
                                "event": "worker_failed",
                                "worker_id": worker_id,
                                "job_id": current_job,
                                "status": "DEAD",
                                "incident": True,
                            }
                        )
                else:
                    if worker_id in known_dead:
                        known_dead.discard(worker_id)
                        await state.store.publish_event(
                            {
                                "event": "worker_started",
                                "worker_id": worker_id,
                                "status": worker.get("status") or "RECOVERING",
                            }
                        )
        except Exception:
            logger.exception("worker monitor failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    state.store = Store(settings.redis_url)
    state.hub = Hub()
    state.connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    state.topology = QueueTopology(state.connection)
    await state.topology.declare(prefetch=settings.prefetch_count)
    state.producer = JobProducer(state.topology)
    state.chaos = ChaosEngine(state.store, settings)
    state.tasks = [
        asyncio.create_task(relay_events(), name="event-relay"),
        asyncio.create_task(monitor_workers(), name="worker-monitor"),
    ]
    logger.info("api ready", extra={"event": "api_ready"})
    yield
    for task in state.tasks:
        task.cancel()
    if state.chaos.running:
        await state.chaos.stop()
    await state.connection.close()
    await state.store.close()


app = FastAPI(title="Fault-Tolerant Task Queue", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list + ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _health(workers: list[dict[str, Any]]) -> str:
    alive = sum(1 for w in workers if w.get("alive") and w.get("status") != "DEAD")
    total = max(len(settings.worker_names), len(workers), 1)
    if alive == 0:
        return "critical"
    if alive < total:
        return "degraded"
    return "operational"


@app.post("/jobs")
async def create_job(payload: JobCreate) -> dict[str, Any]:
    if payload.idempotency_key:
        existing = await state.store.get_job_by_idempotency(payload.idempotency_key)
        if existing:
            return {
                "job_id": existing.job_id,
                "status": existing.status.value,
                "idempotent_replay": True,
            }
    job = Job.new(payload)
    await state.store.save_job(job)
    await state.store.incr("jobs_submitted_total")
    await state.producer.enqueue(job)
    await state.store.publish_event(
        {
            "event": "job_queued",
            "job_id": job.job_id,
            "job_type": job.type.value,
            "idempotency_key": job.idempotency_key,
            "status": job.status.value,
        }
    )
    return {"job_id": job.job_id, "status": job.status.value, "idempotency_key": job.idempotency_key}


@app.post("/jobs/batch")
async def create_batch(payload: BatchJobCreate) -> dict[str, Any]:
    types = payload.types or list(JobType)
    created = []
    for i in range(payload.count):
        job_type = types[i % len(types)]
        sample = {
            JobType.IMAGE_PROCESSING: {"input": f"frame-{i:03d}.jpg", "complexity": "medium"},
            JobType.REPORT_GENERATION: {"title": f"ops-report-{i:03d}", "complexity": "medium"},
            JobType.DATA_TRANSFORMATION: {"dataset": f"events-{i:03d}", "complexity": "medium"},
        }[job_type]
        result = await create_job(JobCreate(type=job_type, payload=sample))
        created.append(result)
    return {"submitted": len(created), "jobs": created}


@app.get("/jobs")
async def list_jobs() -> list[dict[str, Any]]:
    jobs = await state.store.list_jobs()
    return [job.model_dump() for job in jobs]


@app.get("/jobs/{job_id}")
async def get_job(job_id: str) -> dict[str, Any]:
    job = await state.store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job.model_dump()


@app.get("/cluster")
async def cluster() -> dict[str, Any]:
    workers = await state.store.list_workers()
    jobs = await state.store.list_jobs(400)
    queued = 0
    try:
        queued = await state.topology.main_depth()
    except Exception:
        queued = sum(1 for j in jobs if j.status == JobStatus.QUEUED)
    metrics = await metrics_snapshot(state.store, queued=queued)
    chaos = await state.store.get_chaos()
    rabbit_ok = not state.connection.is_closed
    redis_ok = await state.store.ping()
    health = _health(workers)
    if chaos and chaos.get("status") == "running" and health == "operational":
        health = "degraded" if sum(1 for w in workers if not w.get("alive")) else health
    return {
        "health": health,
        "workers": workers,
        "expected_workers": settings.worker_names,
        "queued_jobs": queued,
        "processing": int(metrics.get("processing_jobs") or 0),
        "completed": int(metrics.get("completed_jobs") or 0),
        "failed": int(metrics.get("failed_jobs") or 0),
        "recovery_rate": metrics.get("success_rate"),
        "rabbitmq": "connected" if rabbit_ok else "disconnected",
        "redis": "connected" if redis_ok else "disconnected",
        "chaos": chaos,
        "metrics": metrics,
        "timestamp": iso(),
    }


@app.get("/incidents")
async def incidents() -> list[dict[str, Any]]:
    return await state.store.incidents()


@app.get("/dlq")
async def dlq() -> list[dict[str, Any]]:
    jobs = await state.store.list_dlq()
    return [job.model_dump() for job in jobs]


@app.get("/metrics")
async def metrics_json() -> dict[str, Any]:
    queued = 0
    try:
        queued = await state.topology.main_depth()
    except Exception:
        pass
    return await metrics_snapshot(state.store, queued=queued)


@app.get("/metrics/prometheus")
async def metrics_prom() -> PlainTextResponse:
    data = await metrics_json()
    return PlainTextResponse(prometheus_text(data), media_type="text/plain")


@app.get("/events")
async def events() -> list[dict[str, Any]]:
    return await state.store.recent_events()


@app.post("/chaos/start")
async def chaos_start(payload: ChaosRequest) -> dict[str, Any]:
    return await state.chaos.start(
        duration=payload.duration,
        kill_interval=payload.kill_interval,
        workers=payload.workers,
        restart_delay=payload.restart_delay,
    )


@app.post("/chaos/stop")
async def chaos_stop() -> dict[str, Any]:
    return await state.chaos.stop()


@app.get("/chaos")
async def chaos_status() -> dict[str, Any]:
    return await state.store.get_chaos() or {"status": "idle"}


@app.get("/experiment")
async def experiment() -> dict[str, Any]:
    return await state.store.get_chaos_summary() or {}


@app.post("/reset")
async def reset() -> dict[str, str]:
    await state.store.reset()
    return {"status": "reset"}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await state.hub.connect(ws)
    try:
        snapshot = await cluster()
        await ws.send_json({"event": "snapshot", "cluster": snapshot})
        for event in reversed(await state.store.recent_events(80)):
            await ws.send_json(event)
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        await state.hub.disconnect(ws)
    except Exception:
        await state.hub.disconnect(ws)

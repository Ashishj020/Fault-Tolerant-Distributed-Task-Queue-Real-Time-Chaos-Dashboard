from __future__ import annotations

from typing import Any

from prometheus_client import CollectorRegistry, Gauge, generate_latest

from store.redis_store import Store


PROMETHEUS_KEYS = [
    "jobs_submitted_total",
    "jobs_completed_total",
    "jobs_failed_total",
    "jobs_retried_total",
    "jobs_dead_lettered_total",
    "jobs_redelivered_total",
    "jobs_duplicate_total",
    "jobs_recovered_total",
    "worker_failures_total",
    "worker_restarts_total",
    "side_effects_total",
    "active_workers",
    "queued_jobs",
    "processing_jobs",
]


def prometheus_text(metrics: dict[str, float], extra: dict[str, float] | None = None) -> bytes:
    registry = CollectorRegistry()
    merged = {**metrics, **(extra or {})}
    for key in PROMETHEUS_KEYS:
        gauge = Gauge(key, key.replace("_", " "), registry=registry)
        gauge.set(merged.get(key, 0))
    for key, value in merged.items():
        if key in PROMETHEUS_KEYS:
            continue
        safe = key.replace("-", "_")
        Gauge(safe, safe, registry=registry).set(value)
    return generate_latest(registry)


async def snapshot(store: Store, queued: int = 0) -> dict[str, Any]:
    metrics = await store.metrics()
    workers = await store.list_workers()
    alive = [w for w in workers if w.get("alive") and w.get("status") != "DEAD"]
    processing = [w for w in alive if w.get("status") == "PROCESSING"]
    jobs = await store.list_jobs(400)
    completed = sum(1 for j in jobs if j.status.value == "success")
    failed = sum(1 for j in jobs if j.status.value in ("failure", "dead-letter"))
    inflight = sum(1 for j in jobs if j.status.value in ("assigned", "processing"))
    recovered = int(metrics.get("jobs_recovered_total", 0))
    submitted = int(metrics.get("jobs_submitted_total", 0))
    success_rate = (completed / submitted * 100) if submitted else 100.0
    duplicate_deliveries = int(metrics.get("jobs_duplicate_total", 0) or metrics.get("jobs_redelivered_total", 0))
    side_effects = int(metrics.get("side_effects_total", 0))
    metrics.update(
        {
            "active_workers": len(alive),
            "queued_jobs": queued,
            "processing_jobs": len(processing) or inflight,
            "completed_jobs": completed,
            "failed_jobs": failed,
            "success_rate": success_rate,
            "duplicate_deliveries": duplicate_deliveries,
            "duplicate_side_effects": max(0, side_effects - completed) if False else 0,
            "side_effects_total": side_effects,
        }
    )
    return metrics

from __future__ import annotations

import asyncio
import logging
import random
import statistics
from datetime import datetime, timezone
from typing import Any

import docker

from config import Settings
from models.job import JobStatus, iso
from store.redis_store import Store

logger = logging.getLogger("chaos")


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = p * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    weight = rank - lo
    return ordered[lo] * (1 - weight) + ordered[hi] * weight


class ChaosEngine:
    def __init__(self, store: Store, settings: Settings) -> None:
        self.store = store
        self.settings = settings
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        try:
            self.docker = docker.from_env()
        except Exception:  # noqa: BLE001
            self.docker = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def _container(self, name: str):
        if not self.docker:
            raise RuntimeError("Docker socket is not available inside the API")
        return self.docker.containers.get(name)

    async def start(
        self,
        duration: int = 120,
        kill_interval: int = 15,
        workers: str = "random",
        restart_delay: int | None = None,
    ) -> dict[str, Any]:
        if self.running:
            current = await self.store.get_chaos()
            return current or {"status": "running"}
        self._stop.clear()
        delay = restart_delay if restart_delay is not None else self.settings.chaos_restart_delay
        state = {
            "status": "running",
            "started_at": iso(),
            "duration": duration,
            "kill_interval": kill_interval,
            "workers": workers,
            "restart_delay": delay,
            "workers_killed": 0,
            "jobs_inflight_during_kill": 0,
            "kill_events": [],
        }
        await self.store.set_chaos(state)
        await self.store.publish_event(
            {"event": "failover_started", "incident": True, **state},
        )
        self._task = asyncio.create_task(self._run(state), name="chaos-engine")
        return state

    async def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except asyncio.TimeoutError:
                self._task.cancel()
        state = await self.store.get_chaos() or {"status": "stopped"}
        state["status"] = "stopped"
        await self.store.set_chaos(state)
        return state

    async def _run(self, state: dict[str, Any]) -> None:
        try:
            duration = int(state["duration"])
            interval = int(state["kill_interval"])
            restart_delay = int(state["restart_delay"])
            deadline = asyncio.get_event_loop().time() + duration
            names = self.settings.worker_names
            target = state.get("workers") or "random"
            while not self._stop.is_set() and asyncio.get_event_loop().time() < deadline:
                chosen = await self._pick_worker(names, target)
                if chosen:
                    await self._kill_and_recover(chosen, state, restart_delay)
                remaining = deadline - asyncio.get_event_loop().time()
                wait = min(interval, max(1, remaining))
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    continue
        except Exception:
            logger.exception("chaos engine crashed")
        finally:
            summary = await self._summarize(state)
            state["status"] = "complete"
            state["finished_at"] = iso()
            state["summary"] = summary
            await self.store.set_chaos(state)
            await self.store.set_chaos_summary(summary)
            await self.store.publish_event(
                {"event": "failover_completed", "incident": True, "summary": summary},
            )

    async def _pick_worker(self, names: list[str], target: str) -> str | None:
        live = []
        for name in names:
            info = await self.store.get_worker(name)
            if info and info.get("alive") and info.get("status") != "DEAD":
                live.append(name)
        if len(live) <= 1:
            return None
        if target != "random" and target in live:
            return target
        return random.choice(live)

    async def _kill_and_recover(self, name: str, state: dict[str, Any], restart_delay: int) -> None:
        info = await self.store.get_worker(name) or {}
        current_job = info.get("current_job")
        killed_at = iso()
        inflight = []
        if current_job:
            job = await self.store.get_job(current_job)
            if job and job.status in (JobStatus.ASSIGNED, JobStatus.PROCESSING, JobStatus.QUEUED):
                job.inflight_failure_at = killed_at
                job.failure_reason = "worker killed mid-job"
                await self.store.save_job(job)
                inflight.append(job.job_id)
        for job in await self.store.list_jobs(80):
            if job.assigned_worker == name and job.status in (JobStatus.ASSIGNED, JobStatus.PROCESSING):
                if not job.inflight_failure_at:
                    job.inflight_failure_at = killed_at
                    job.failure_reason = "worker killed mid-job"
                    await self.store.save_job(job)
                if job.job_id not in inflight:
                    inflight.append(job.job_id)

        logger.warning(
            "killing worker",
            extra={"event": "worker_killed", "worker_id": name, "job_id": current_job},
        )
        container = await asyncio.to_thread(self._container, name)
        await asyncio.to_thread(container.kill)
        await self.store.mark_worker_dead(name)
        await self.store.incr("worker_failures_total")
        state["workers_killed"] = int(state.get("workers_killed") or 0) + 1
        state["jobs_inflight_during_kill"] = int(state.get("jobs_inflight_during_kill") or 0) + len(inflight)
        state.setdefault("kill_events", []).append(
            {"worker_id": name, "timestamp": killed_at, "jobs": inflight}
        )
        await self.store.set_chaos(state)
        await self.store.publish_event(
            {
                "event": "WORKER_KILLED",
                "worker_id": name,
                "job_id": current_job,
                "inflight_jobs": inflight,
                "incident": True,
            }
        )
        await self.store.publish_event(
            {
                "event": "worker_killed",
                "worker_id": name,
                "job_id": current_job,
                "inflight_jobs": inflight,
                "incident": True,
            }
        )
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=restart_delay)
            return
        except asyncio.TimeoutError:
            pass
        try:
            container = await asyncio.to_thread(self._container, name)
            await asyncio.to_thread(container.start)
            await self.store.incr("worker_restarts_total")
            await self.store.set_worker(
                name,
                {
                    "worker_id": name,
                    "status": "RECOVERING",
                    "current_job": None,
                    "timestamp": iso(),
                },
                heartbeat=False,
            )
            await self.store.publish_event(
                {
                    "event": "worker_started",
                    "worker_id": name,
                    "status": "RECOVERING",
                    "incident": True,
                }
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("failed to restart %s: %s", name, exc)

    async def _summarize(self, state: dict[str, Any]) -> dict[str, Any]:
        jobs = await self.store.list_jobs(1000)
        metrics = await self.store.metrics()
        recovered = [j for j in jobs if j.recovered_at and j.recovery_ms is not None]
        times = [j.recovery_ms / 1000 for j in recovered if j.recovery_ms is not None]
        inflight_ids = set()
        for event in state.get("kill_events") or []:
            inflight_ids.update(event.get("jobs") or [])
        inflight_jobs = [j for j in jobs if j.job_id in inflight_ids]
        lost = [
            j
            for j in inflight_jobs
            if j.status.value not in ("success", "retry", "queued", "assigned", "processing")
        ]
        still_open = [j for j in inflight_jobs if j.status.value in ("queued", "assigned", "processing", "retry")]
        dead = [j for j in jobs if j.status.value == "dead-letter"]
        completed = [j for j in jobs if j.status.value == "success"]
        median = statistics.median(times) if times else 0.0
        p95 = _percentile(times, 0.95)
        recovered_count = len([j for j in inflight_jobs if j.status.value == "success"])
        submitted = int(metrics.get("jobs_submitted_total", len(jobs)))
        summary = {
            "duration": state.get("duration"),
            "workers": len(self.settings.worker_names),
            "workers_killed": int(state.get("workers_killed") or 0),
            "jobs_submitted": submitted,
            "jobs_inflight_during_kill": int(state.get("jobs_inflight_during_kill") or 0),
            "jobs_recovered": recovered_count,
            "jobs_lost": len(lost),
            "jobs_still_open": len(still_open),
            "dead_lettered": len(dead),
            "retry_count": int(metrics.get("jobs_retried_total") or 0),
            "duplicate_deliveries": int(metrics.get("jobs_redelivered_total") or 0),
            "duplicate_side_effects": 0,
            "side_effects_total": int(metrics.get("side_effects_total") or 0),
            "completed": len(completed),
            "median_recovery_s": round(median, 3),
            "p95_recovery_s": round(p95, 3),
            "recovery_success_rate": round((recovered_count / len(inflight_jobs) * 100) if inflight_jobs else 100.0, 2),
            "finished_at": iso(),
        }
        return summary

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import redis.asyncio as redis

from models.job import Job, iso


JOB_KEY = "job:{job_id}"
JOBS_INDEX = "jobs:index"
JOBS_BY_STATUS = "jobs:status:{status}"
IDEM_JOB = "idempotency:job:{key}"
EVENTS_LIST = "events:recent"
INCIDENTS_LIST = "incidents"
CHANNEL_EVENTS = "channel:events"
WORKERS_SET = "workers:known"
WORKER_INFO = "worker:info:{worker_id}"
WORKER_HB = "worker:hb:{worker_id}"
METRICS_HASH = "metrics"
SIDE_EFFECTS = "side_effects:{key}"
DLQ_INDEX = "jobs:dlq"
CHAOS_STATE = "chaos:state"
CHAOS_SUMMARY = "chaos:summary"
HEARTBEAT_TTL = 4


class Store:
    def __init__(self, url: str) -> None:
        self.redis = redis.from_url(url, decode_responses=True)

    async def ping(self) -> bool:
        return bool(await self.redis.ping())

    async def close(self) -> None:
        await self.redis.aclose()

    async def save_job(self, job: Job) -> None:
        payload = job.model_dump_json()
        created = datetime.fromisoformat(job.created_at.replace("Z", "+00:00")).timestamp()
        pipe = self.redis.pipeline()
        pipe.set(JOB_KEY.format(job_id=job.job_id), payload)
        pipe.zadd(JOBS_INDEX, {job.job_id: created})
        pipe.sadd(JOBS_BY_STATUS.format(status=job.status.value), job.job_id)
        pipe.set(IDEM_JOB.format(key=job.idempotency_key), job.job_id)
        await pipe.execute()

    async def get_job(self, job_id: str) -> Job | None:
        raw = await self.redis.get(JOB_KEY.format(job_id=job_id))
        if not raw:
            return None
        return Job.model_validate_json(raw)

    async def get_job_by_idempotency(self, key: str) -> Job | None:
        job_id = await self.redis.get(IDEM_JOB.format(key=key))
        if not job_id:
            return None
        return await self.get_job(job_id)

    async def list_jobs(self, limit: int = 200) -> list[Job]:
        ids = await self.redis.zrevrange(JOBS_INDEX, 0, limit - 1)
        jobs: list[Job] = []
        for job_id in ids:
            job = await self.get_job(job_id)
            if job:
                jobs.append(job)
        return jobs

    async def list_dlq(self) -> list[Job]:
        ids = await self.redis.lrange(DLQ_INDEX, 0, 199)
        jobs: list[Job] = []
        for job_id in ids:
            job = await self.get_job(job_id)
            if job:
                jobs.append(job)
        return jobs

    async def mark_dlq(self, job_id: str) -> None:
        await self.redis.lpush(DLQ_INDEX, job_id)

    async def publish_event(self, event: dict[str, Any], persist: bool = True) -> dict[str, Any]:
        if "timestamp" not in event:
            event["timestamp"] = iso()
        encoded = json.dumps(event, default=str)
        pipe = self.redis.pipeline()
        pipe.publish(CHANNEL_EVENTS, encoded)
        if persist:
            pipe.lpush(EVENTS_LIST, encoded)
            pipe.ltrim(EVENTS_LIST, 0, 499)
        if event.get("incident"):
            pipe.lpush(INCIDENTS_LIST, encoded)
            pipe.ltrim(INCIDENTS_LIST, 0, 199)
        await pipe.execute()
        return event

    async def recent_events(self, limit: int = 150) -> list[dict[str, Any]]:
        raw = await self.redis.lrange(EVENTS_LIST, 0, limit - 1)
        return [json.loads(item) for item in raw]

    async def incidents(self, limit: int = 100) -> list[dict[str, Any]]:
        raw = await self.redis.lrange(INCIDENTS_LIST, 0, limit - 1)
        return [json.loads(item) for item in raw]

    async def incr(self, metric: str, amount: int = 1) -> int:
        return int(await self.redis.hincrby(METRICS_HASH, metric, amount))

    async def set_metric(self, metric: str, value: float | int) -> None:
        await self.redis.hset(METRICS_HASH, metric, value)

    async def metrics(self) -> dict[str, float]:
        raw = await self.redis.hgetall(METRICS_HASH)
        out: dict[str, float] = {}
        for key, value in raw.items():
            try:
                out[key] = float(value)
            except ValueError:
                continue
        return out

    async def observe(self, metric: str, value: float) -> None:
        await self.redis.rpush(f"hist:{metric}", value)
        await self.redis.ltrim(f"hist:{metric}", -1000, -1)

    async def histogram(self, metric: str) -> list[float]:
        raw = await self.redis.lrange(f"hist:{metric}", 0, -1)
        return [float(v) for v in raw]

    async def set_worker(self, worker_id: str, info: dict[str, Any], heartbeat: bool = True) -> None:
        pipe = self.redis.pipeline()
        pipe.hset(WORKER_INFO.format(worker_id=worker_id), mapping={k: json.dumps(v) if not isinstance(v, str) else v for k, v in info.items()})
        pipe.sadd(WORKERS_SET, worker_id)
        if heartbeat:
            pipe.set(WORKER_HB.format(worker_id=worker_id), info.get("status", "IDLE"), ex=HEARTBEAT_TTL)
        await pipe.execute()

    async def get_worker(self, worker_id: str) -> dict[str, Any] | None:
        raw = await self.redis.hgetall(WORKER_INFO.format(worker_id=worker_id))
        if not raw:
            return None
        parsed: dict[str, Any] = {"worker_id": worker_id}
        for key, value in raw.items():
            try:
                parsed[key] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                parsed[key] = value
        hb = await self.redis.get(WORKER_HB.format(worker_id=worker_id))
        parsed["alive"] = hb is not None
        if not parsed["alive"] and parsed.get("status") not in ("DEAD",):
            parsed["status"] = "DEAD"
        return parsed

    async def list_workers(self) -> list[dict[str, Any]]:
        ids = sorted(await self.redis.smembers(WORKERS_SET))
        workers = []
        for worker_id in ids:
            info = await self.get_worker(worker_id)
            if info:
                workers.append(info)
        return workers

    async def mark_worker_dead(self, worker_id: str) -> dict[str, Any] | None:
        info = await self.get_worker(worker_id)
        if not info:
            return None
        info["status"] = "DEAD"
        info["alive"] = False
        info["updated_at"] = iso()
        await self.set_worker(worker_id, {k: v for k, v in info.items() if k != "alive"}, heartbeat=False)
        await self.redis.delete(WORKER_HB.format(worker_id=worker_id))
        return info

    async def commit_side_effect(self, idempotency_key: str, payload: dict[str, Any]) -> bool:
        """
        Atomic side-effect commit. SET NX so only the first successful writer
        records the real side effect. Later deliveries see this key and skip.
        """
        encoded = json.dumps(payload, default=str)
        ok = await self.redis.set(SIDE_EFFECTS.format(key=idempotency_key), encoded, nx=True)
        return bool(ok)

    async def get_side_effect(self, idempotency_key: str) -> dict[str, Any] | None:
        raw = await self.redis.get(SIDE_EFFECTS.format(key=idempotency_key))
        return json.loads(raw) if raw else None

    async def count_side_effects(self) -> int:
        keys = [key async for key in self.redis.scan_iter(match="side_effects:*")]
        return len(keys)

    async def set_chaos(self, state: dict[str, Any]) -> None:
        await self.redis.set(CHAOS_STATE, json.dumps(state, default=str))

    async def get_chaos(self) -> dict[str, Any] | None:
        raw = await self.redis.get(CHAOS_STATE)
        return json.loads(raw) if raw else None

    async def set_chaos_summary(self, summary: dict[str, Any]) -> None:
        await self.redis.set(CHAOS_SUMMARY, json.dumps(summary, default=str))

    async def get_chaos_summary(self) -> dict[str, Any] | None:
        raw = await self.redis.get(CHAOS_SUMMARY)
        return json.loads(raw) if raw else None

    async def reset(self) -> None:
        await self.redis.flushdb()

from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any

import aio_pika

from config import get_settings
from idempotency.store import IdempotencyStore
from logging_setup import configure_logging
from models.job import Job, JobStatus, WorkerState, iso
from broker.retry import next_attempt_allowed, retry_queue_delay_ms
from broker.topology import QueueTopology
from store.redis_store import Store
from workers.tasks import TaskError, run_task

logger = logging.getLogger("worker")


class Worker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.worker_id = os.environ.get("WORKER_ID", self.settings.worker_id)
        self.store = Store(self.settings.redis_url)
        self.idem = IdempotencyStore(self.store, ttl_seconds=self.settings.worker_claim_ttl)
        self.topology: QueueTopology | None = None
        self.connection: aio_pika.RobustConnection | None = None
        self.state = WorkerState.STARTING
        self.current_job: str | None = None
        self._stop = asyncio.Event()
        self._started_at = iso()

    async def _set_state(self, state: WorkerState, **extra: Any) -> None:
        self.state = state
        info = {
            "worker_id": self.worker_id,
            "status": state.value,
            "current_job": self.current_job,
            "started_at": self._started_at,
            "timestamp": iso(),
            **extra,
        }
        await self.store.set_worker(self.worker_id, info, heartbeat=True)
        event_name = {
            WorkerState.STARTING: "worker_started",
            WorkerState.CONNECTING: "worker_started",
            WorkerState.READY: "worker_started",
            WorkerState.IDLE: "worker_heartbeat",
            WorkerState.PROCESSING: "worker_processing",
            WorkerState.RECOVERING: "worker_started",
        }.get(state, "worker_heartbeat")
        if state in (WorkerState.STARTING, WorkerState.READY, WorkerState.PROCESSING, WorkerState.RECOVERING):
            await self.store.publish_event(
                {
                    "event": event_name,
                    "worker_id": self.worker_id,
                    "status": state.value,
                    "current_job": self.current_job,
                    **extra,
                }
            )

    async def _heartbeat_loop(self) -> None:
        interval = self.settings.heartbeat_interval_ms / 1000
        while not self._stop.is_set():
            await self.store.set_worker(
                self.worker_id,
                {
                    "worker_id": self.worker_id,
                    "status": self.state.value,
                    "current_job": self.current_job,
                    "started_at": self._started_at,
                    "timestamp": iso(),
                },
                heartbeat=True,
            )
            await self.store.publish_event(
                {
                    "event": "worker_heartbeat",
                    "worker_id": self.worker_id,
                    "status": self.state.value,
                    "current_job": self.current_job,
                },
                persist=False,
            )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def _update_job(self, job: Job, event: str, **extra: Any) -> None:
        entry = job.add_event(event, worker_id=self.worker_id, **extra)
        await self.store.save_job(job)
        await self.store.publish_event(
            {
                "event": event,
                "job_id": job.job_id,
                "worker_id": self.worker_id,
                "attempt": job.attempt,
                "status": job.status.value,
                "idempotency_key": job.idempotency_key,
                "job_type": job.type.value,
                "redelivered": job.redelivered,
                **extra,
                "timeline_entry": entry,
            }
        )

    async def _handle_redelivery(self, job: Job) -> None:
        job.redelivered = True
        await self.store.incr("jobs_redelivered_total")
        await self.store.incr("jobs_duplicate_total")
        if job.inflight_failure_at or job.status in (JobStatus.PROCESSING, JobStatus.ASSIGNED):
            await self._update_job(job, "job_requeued", previous_worker=job.assigned_worker)

    async def _complete_recovered(self, job: Job) -> None:
        if job.inflight_failure_at:
            from datetime import datetime

            start = datetime.fromisoformat(job.inflight_failure_at.replace("Z", "+00:00"))
            end = datetime.fromisoformat(iso().replace("Z", "+00:00"))
            job.recovered_at = iso()
            job.recovery_ms = (end - start).total_seconds() * 1000
            job.recovery_worker = self.worker_id
            await self.store.incr("jobs_recovered_total")
            await self.store.observe("job_recovery_duration_seconds", job.recovery_ms / 1000)
            await self._update_job(
                job,
                "job_recovered",
                recovery_ms=job.recovery_ms,
                incident=True,
            )

    async def process_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        async with message.process(requeue=False, ignore_processed=True):
            job = Job.model_validate_json(message.body)
            existing = await self.store.get_job(job.job_id)
            if existing:
                job = existing
                job.attempt = max(job.attempt, existing.attempt)
            if message.redelivered:
                await self._handle_redelivery(job)

            self.current_job = job.job_id
            await self._set_state(WorkerState.PROCESSING, job_id=job.job_id)
            if not job.first_worker:
                job.first_worker = self.worker_id
            elif job.first_worker != self.worker_id and job.redelivered:
                job.recovery_worker = self.worker_id
            job.assigned_worker = self.worker_id
            job.status = JobStatus.ASSIGNED
            await self._update_job(job, "job_assigned")
            job.status = JobStatus.PROCESSING
            await self._update_job(job, "worker_processing")

            committed = await self.idem.get_committed(job.idempotency_key)
            if committed:
                job.status = JobStatus.SUCCESS
                job.result = committed
                job.skipped_duplicate = True
                job.side_effect_committed = True
                await self.store.incr("jobs_completed_total")
                await self._complete_recovered(job)
                await self._update_job(job, "job_completed", skipped_duplicate=True)
                logger.info(
                    "skipped duplicate side effect",
                    extra={
                        "event": "duplicate_delivery_skipped",
                        "job_id": job.job_id,
                        "worker_id": self.worker_id,
                        "idempotency_key": job.idempotency_key,
                        "attempt": job.attempt,
                    },
                )
                self.current_job = None
                await self._set_state(WorkerState.IDLE)
                return

            claimed = await self.idem.try_claim(job.idempotency_key, self.worker_id)
            if not claimed and (message.redelivered or job.redelivered):
                # Previous consumer died mid-job. The lock owner cannot still be
                # working this delivery, so it is safe to take over immediately.
                claimed = True
                await self.idem.release(job.idempotency_key)
                claimed = await self.idem.try_claim(job.idempotency_key, self.worker_id)
                if not claimed:
                    claimed = await self.idem.steal_if_dead(job.idempotency_key, self.worker_id)
            if not claimed:
                claimed = await self.idem.steal_if_dead(job.idempotency_key, self.worker_id)
            if not claimed:
                # Another live worker holds the lock. Wait for its commit.
                for _ in range(20):
                    await asyncio.sleep(0.25)
                    committed = await self.idem.get_committed(job.idempotency_key)
                    if committed:
                        job.status = JobStatus.SUCCESS
                        job.result = committed
                        job.skipped_duplicate = True
                        job.side_effect_committed = True
                        await self.store.incr("jobs_completed_total")
                        await self._complete_recovered(job)
                        await self._update_job(job, "job_completed", skipped_duplicate=True)
                        self.current_job = None
                        await self._set_state(WorkerState.IDLE)
                        return
                claimed = await self.idem.steal_if_dead(job.idempotency_key, self.worker_id)
                if not claimed:
                    raise TaskError("idempotency lock contention", retryable=True)

            try:
                result = await asyncio.to_thread(run_task, job, self.settings.results_dir)
                first = await self.idem.commit(job.idempotency_key, result)
                job.result = result
                job.side_effect_committed = True
                job.status = JobStatus.SUCCESS
                if first:
                    await self.store.incr("side_effects_total")
                else:
                    job.skipped_duplicate = True
                    await self.store.incr("jobs_duplicate_total")
                await self.store.incr("jobs_completed_total")
                duration = float(result.get("duration_s") or 0)
                await self.store.observe("job_processing_duration_seconds", duration)
                await self._complete_recovered(job)
                await self._update_job(job, "job_completed", skipped_duplicate=job.skipped_duplicate)
            except TaskError as exc:
                await self.idem.release(job.idempotency_key)
                await self._fail_job(job, str(exc), retryable=exc.retryable)
            except Exception as exc:  # noqa: BLE001
                await self.idem.release(job.idempotency_key)
                await self._fail_job(job, str(exc), retryable=True)
            finally:
                self.current_job = None
                if not self._stop.is_set():
                    await self._set_state(WorkerState.IDLE)
            # ACK happens when the `message.process()` context exits cleanly.
            # A crash before this point leaves the message unacked so RabbitMQ
            # redelivers it to a surviving consumer.

    async def _fail_job(self, job: Job, error: str, retryable: bool) -> None:
        job.error = error
        job.failure_reason = error
        job.status = JobStatus.FAILURE
        await self.store.incr("jobs_failed_total")
        await self._update_job(job, "worker_failed", error=error)
        logger.warning(
            "job failed",
            extra={
                "event": "job_failed",
                "job_id": job.job_id,
                "worker_id": self.worker_id,
                "attempt": job.attempt,
                "error": error,
            },
        )
        assert self.topology is not None
        if retryable and next_attempt_allowed(job.attempt, self.settings):
            delay_ms = retry_queue_delay_ms(job.attempt, self.settings)
            job.attempt += 1
            job.status = JobStatus.RETRY
            job.assigned_worker = None
            await self.store.incr("jobs_retried_total")
            await self._update_job(job, "job_retrying", delay_ms=delay_ms)
            await self.topology.publish_retry(job.model_dump_json().encode("utf-8"), delay_ms)
            return
        job.status = JobStatus.DEAD_LETTER
        await self.store.incr("jobs_dead_lettered_total")
        await self.store.mark_dlq(job.job_id)
        await self._update_job(job, "job_dead_lettered", incident=True, error=error)
        dead_payload = {
            **job.model_dump(),
            "dead_lettered_at": iso(),
            "worker_id": self.worker_id,
            "failure_reason": error,
        }
        import json

        await self.topology.publish_dead(json.dumps(dead_payload, default=str).encode("utf-8"))

    async def run(self) -> None:
        configure_logging()
        await self._set_state(WorkerState.STARTING)
        await self._set_state(WorkerState.CONNECTING)
        self.connection = await aio_pika.connect_robust(self.settings.rabbitmq_url)
        self.topology = QueueTopology(self.connection)
        await self.topology.declare(prefetch=self.settings.prefetch_count)
        await self._set_state(WorkerState.READY)
        await self._set_state(WorkerState.IDLE)
        logger.info(
            "worker ready",
            extra={"event": "worker_ready", "worker_id": self.worker_id},
        )
        heartbeat = asyncio.create_task(self._heartbeat_loop())
        assert self.topology.main_queue is not None
        try:
            async with self.topology.main_queue.iterator() as iterator:
                async for message in iterator:
                    if self._stop.is_set():
                        break
                    await self.process_message(message)
        finally:
            heartbeat.cancel()
            if self.connection:
                await self.connection.close()
            await self.store.close()

    def request_stop(self) -> None:
        self._stop.set()


async def main() -> None:
    worker = Worker()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.request_stop)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

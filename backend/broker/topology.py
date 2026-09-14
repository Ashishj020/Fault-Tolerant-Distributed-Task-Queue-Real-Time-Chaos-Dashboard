from __future__ import annotations

import aio_pika
from aio_pika import ExchangeType

EXCHANGE_JOBS = "jobs.direct"
EXCHANGE_RETRY = "jobs.retry"
EXCHANGE_REQUEUE = "jobs.requeue"
EXCHANGE_DLX = "jobs.dlx"

QUEUE_MAIN = "jobs.main"
QUEUE_DEAD = "jobs.dead"
ROUTING_JOBS = "jobs"
ROUTING_DEAD = "dead"

# Dedicated TTL retry queues. Per-message TTL is unsafe in a shared FIFO queue
# because a long-TTL message at the head blocks shorter-TTL messages.
RETRY_DELAYS_MS = (2000, 4000, 8000, 16000)


class QueueTopology:
    def __init__(self, connection: aio_pika.RobustConnection) -> None:
        self.connection = connection
        self.channel: aio_pika.abc.AbstractRobustChannel | None = None
        self.jobs_exchange: aio_pika.abc.AbstractRobustExchange | None = None
        self.retry_exchange: aio_pika.abc.AbstractRobustExchange | None = None
        self.requeue_exchange: aio_pika.abc.AbstractRobustExchange | None = None
        self.dlx_exchange: aio_pika.abc.AbstractRobustExchange | None = None
        self.main_queue: aio_pika.abc.AbstractRobustQueue | None = None
        self.dead_queue: aio_pika.abc.AbstractRobustQueue | None = None

    async def declare(self, prefetch: int = 1) -> None:
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=prefetch)

        self.jobs_exchange = await self.channel.declare_exchange(
            EXCHANGE_JOBS, ExchangeType.DIRECT, durable=True
        )
        self.retry_exchange = await self.channel.declare_exchange(
            EXCHANGE_RETRY, ExchangeType.DIRECT, durable=True
        )
        self.requeue_exchange = await self.channel.declare_exchange(
            EXCHANGE_REQUEUE, ExchangeType.DIRECT, durable=True
        )
        self.dlx_exchange = await self.channel.declare_exchange(
            EXCHANGE_DLX, ExchangeType.DIRECT, durable=True
        )

        self.main_queue = await self.channel.declare_queue(
            QUEUE_MAIN,
            durable=True,
            arguments={
                "x-dead-letter-exchange": EXCHANGE_DLX,
                "x-dead-letter-routing-key": ROUTING_DEAD,
            },
        )
        await self.main_queue.bind(self.jobs_exchange, routing_key=ROUTING_JOBS)
        await self.main_queue.bind(self.requeue_exchange, routing_key=ROUTING_JOBS)

        self.dead_queue = await self.channel.declare_queue(QUEUE_DEAD, durable=True)
        await self.dead_queue.bind(self.dlx_exchange, routing_key=ROUTING_DEAD)

        for delay in RETRY_DELAYS_MS:
            retry_queue = await self.channel.declare_queue(
                f"jobs.retry.{delay}",
                durable=True,
                arguments={
                    "x-message-ttl": delay,
                    "x-dead-letter-exchange": EXCHANGE_REQUEUE,
                    "x-dead-letter-routing-key": ROUTING_JOBS,
                },
            )
            await retry_queue.bind(self.retry_exchange, routing_key=f"retry.{delay}")

    async def publish_job(self, body: bytes, routing_key: str = ROUTING_JOBS) -> None:
        assert self.jobs_exchange is not None
        await self.jobs_exchange.publish(
            aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
            ),
            routing_key=routing_key,
        )

    async def publish_retry(self, body: bytes, delay_ms: int) -> None:
        assert self.retry_exchange is not None
        await self.retry_exchange.publish(
            aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
            ),
            routing_key=f"retry.{delay_ms}",
        )

    async def publish_dead(self, body: bytes) -> None:
        assert self.dlx_exchange is not None
        await self.dlx_exchange.publish(
            aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
            ),
            routing_key=ROUTING_DEAD,
        )

    async def main_depth(self) -> int:
        assert self.channel is not None
        declared = await self.channel.declare_queue(QUEUE_MAIN, durable=True, passive=True)
        return declared.declaration_result.message_count or 0

    async def dead_depth(self) -> int:
        assert self.channel is not None
        declared = await self.channel.declare_queue(QUEUE_DEAD, durable=True, passive=True)
        return declared.declaration_result.message_count or 0

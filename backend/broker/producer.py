from __future__ import annotations

from models.job import Job
from broker.topology import QueueTopology


class JobProducer:
    def __init__(self, topology: QueueTopology) -> None:
        self.topology = topology

    async def enqueue(self, job: Job) -> None:
        await self.topology.publish_job(job.model_dump_json().encode("utf-8"))

"""
Consumer contract.

Workers MUST use manual acknowledgements and ACK only after:
  - the side effect is committed to the idempotency store, or
  - the job is safely published to a retry / dead-letter queue.

A SIGKILL before ACK leaves the message unacked. RabbitMQ then returns it
to `jobs.main` and a surviving consumer receives it with `redelivered=True`.
"""

from broker.topology import QueueTopology

__all__ = ["QueueTopology"]

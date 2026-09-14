from __future__ import annotations

import random

from config import Settings
from broker.topology import RETRY_DELAYS_MS


def exponential_delay(attempt: int, settings: Settings) -> float:
    """
    delay = base_delay * 2^(attempt - 1), capped, plus jitter.
    Attempt is the attempt that just failed (so next wait uses that attempt number).
    """
    expo = settings.base_retry_delay * (2 ** max(attempt - 1, 0))
    capped = min(expo, settings.max_retry_delay)
    jitter = random.uniform(0, min(0.5, capped * 0.15))
    return capped + jitter


def retry_queue_delay_ms(attempt: int, settings: Settings) -> int:
    delay = exponential_delay(attempt, settings)
    target = int(delay * 1000)
    return min(RETRY_DELAYS_MS, key=lambda item: abs(item - target))


def next_attempt_allowed(attempt: int, settings: Settings) -> bool:
    return attempt < settings.max_retries

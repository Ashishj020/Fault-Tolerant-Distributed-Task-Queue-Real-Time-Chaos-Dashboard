from __future__ import annotations

from typing import Any

from store.redis_store import Store

LOCK_PREFIX = "idempotency:lock:{key}"


class IdempotencyStore:
    def __init__(self, store: Store, ttl_seconds: int = 60) -> None:
        self.store = store
        self.ttl_seconds = ttl_seconds

    async def get_committed(self, key: str) -> dict[str, Any] | None:
        return await self.store.get_side_effect(key)

    async def lock_owner(self, key: str) -> str | None:
        return await self.store.redis.get(LOCK_PREFIX.format(key=key))

    async def try_claim(self, key: str, owner: str) -> bool:
        lock_key = LOCK_PREFIX.format(key=key)
        return bool(await self.store.redis.set(lock_key, owner, nx=True, ex=self.ttl_seconds))

    async def steal_if_dead(self, key: str, owner: str) -> bool:
        current = await self.lock_owner(key)
        if not current:
            return await self.try_claim(key, owner)
        worker = await self.store.get_worker(current)
        if worker and worker.get("alive") and worker.get("status") not in ("DEAD",):
            return False
        await self.release(key)
        return await self.try_claim(key, owner)

    async def release(self, key: str) -> None:
        await self.store.redis.delete(LOCK_PREFIX.format(key=key))

    async def commit(self, key: str, payload: dict[str, Any]) -> bool:
        return await self.store.commit_side_effect(key, payload)

import asyncio
import os
import time
import uuid

import pytest

from idempotency.store import IdempotencyStore
from store.redis_store import Store


def test_duplicate_http_submit_reuses_job(api):
    key = f"idem-{uuid.uuid4().hex[:8]}"
    first = api.post(
        "/jobs",
        json={"type": "report_generation", "payload": {"title": "dup", "duration": 1.0}, "idempotency_key": key},
    ).json()
    second = api.post(
        "/jobs",
        json={"type": "report_generation", "payload": {"title": "dup", "duration": 1.0}, "idempotency_key": key},
    ).json()
    assert first["job_id"] == second["job_id"]
    assert second.get("idempotent_replay") is True


def test_double_delivery_one_side_effect(api):
    key = f"idem-delivery-{uuid.uuid4().hex[:8]}"
    created = api.post(
        "/jobs",
        json={
            "type": "data_transformation",
            "payload": {"dataset": "dup-delivery", "duration": 1.5},
            "idempotency_key": key,
        },
    ).json()
    deadline = time.time() + 40
    job = {}
    while time.time() < deadline:
        job = api.get(f"/jobs/{created['job_id']}").json()
        if job.get("status") == "success":
            break
        time.sleep(0.4)
    assert job.get("status") == "success"
    metrics = api.get("/metrics").json()
    assert int(metrics.get("side_effects_total") or 0) >= 1
    assert int(metrics.get("duplicate_side_effects") or 0) == 0


@pytest.mark.asyncio
async def test_set_nx_prevents_races():
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    store = Store(url)
    try:
        await store.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"redis unavailable: {exc}")
    idem = IdempotencyStore(store, ttl_seconds=10)
    key = f"race-{uuid.uuid4().hex}"

    async def claim(owner: str) -> bool:
        return await idem.try_claim(key, owner)

    results = await asyncio.gather(claim("w1"), claim("w2"), claim("w3"))
    assert sum(1 for item in results if item) == 1
    first = await idem.commit(key, {"ok": True})
    second = await idem.commit(key, {"ok": False})
    assert first is True
    assert second is False
    await store.close()

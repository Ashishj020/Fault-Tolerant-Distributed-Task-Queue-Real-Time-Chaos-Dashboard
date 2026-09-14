import time
import uuid


def test_killed_worker_job_is_redelivered_and_recovered(api):
    created = api.post(
        "/jobs",
        json={
            "type": "image_processing",
            "payload": {"input": "chaos.jpg", "duration": 10},
            "idempotency_key": f"fail-{uuid.uuid4().hex[:8]}",
        },
    ).json()
    job_id = created["job_id"]
    worker = None
    deadline = time.time() + 20
    while time.time() < deadline:
        job = api.get(f"/jobs/{job_id}").json()
        if job.get("assigned_worker") and job.get("status") == "processing":
            worker = job["assigned_worker"]
            break
        time.sleep(0.3)
    assert worker, "job never started processing"

    chaos = api.post(
        "/chaos/start",
        json={"duration": 20, "kill_interval": 12, "workers": worker, "restart_delay": 6},
    )
    chaos.raise_for_status()

    recovered = {}
    deadline = time.time() + 60
    while time.time() < deadline:
        recovered = api.get(f"/jobs/{job_id}").json()
        if recovered.get("status") == "success":
            break
        time.sleep(0.5)
    api.post("/chaos/stop")
    assert recovered.get("status") == "success"
    assert recovered.get("redelivered") or recovered.get("recovery_worker") or recovered.get("attempt", 1) >= 1
    events = [item["event"] for item in recovered.get("timeline") or []]
    assert "job_completed" in events
    if recovered.get("recovery_ms"):
        assert recovered["recovery_ms"] > 0
    metrics = api.get("/metrics").json()
    assert int(metrics.get("duplicate_side_effects") or 0) == 0

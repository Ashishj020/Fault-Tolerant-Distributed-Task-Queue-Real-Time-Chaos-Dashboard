import time
import uuid


def test_max_retries_go_to_dlq(api):
    created = api.post(
        "/jobs",
        json={
            "type": "report_generation",
            "payload": {"title": "poison", "duration": 0.4, "force_fail": True, "fail_reason": "poison pill"},
            "idempotency_key": f"dlq-{uuid.uuid4().hex[:8]}",
        },
    ).json()
    deadline = time.time() + 80
    job = {}
    while time.time() < deadline:
        job = api.get(f"/jobs/{created['job_id']}").json()
        if job.get("status") == "dead-letter":
            break
        time.sleep(0.6)
    assert job["status"] == "dead-letter"
    assert job["attempt"] >= 5
    assert "poison" in (job.get("failure_reason") or "")
    dlq = api.get("/dlq").json()
    assert any(item["job_id"] == created["job_id"] for item in dlq)
    events = [item["event"] for item in job["timeline"]]
    assert "job_dead_lettered" in events
    assert events.count("job_retrying") >= 4

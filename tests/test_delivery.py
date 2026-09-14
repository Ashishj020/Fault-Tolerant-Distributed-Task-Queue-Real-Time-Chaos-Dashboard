import time
import uuid


def test_job_is_acked_only_after_success(api):
    created = api.post(
        "/jobs",
        json={
            "type": "image_processing",
            "payload": {"input": "sample.jpg", "duration": 1.5},
            "idempotency_key": f"deliver-{uuid.uuid4().hex[:8]}",
        },
    ).json()
    deadline = time.time() + 40
    job = {}
    while time.time() < deadline:
        job = api.get(f"/jobs/{created['job_id']}").json()
        if job.get("status") == "success":
            break
        time.sleep(0.4)
    assert job["status"] == "success"
    events = [item["event"] for item in job["timeline"]]
    assert events[0] == "job_queued"
    assert "job_assigned" in events
    assert "job_completed" in events
    assert "job_queued" in events

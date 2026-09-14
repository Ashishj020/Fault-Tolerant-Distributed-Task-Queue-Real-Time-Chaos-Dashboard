import time

from broker.retry import exponential_delay, retry_queue_delay_ms
from config import Settings


def wait_status(api, job_id: str, wanted: set[str], timeout: float = 90.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = api.get(f"/jobs/{job_id}").json()
        if last.get("status") in wanted:
            return last
        time.sleep(0.5)
    raise AssertionError(f"job {job_id} stayed {last.get('status')} not in {wanted}")


def test_exponential_backoff_formula():
    settings = Settings(base_retry_delay=2, max_retry_delay=16)
    assert 2 <= exponential_delay(1, settings) <= 2.5
    assert 4 <= exponential_delay(2, settings) <= 4.6
    assert 8 <= exponential_delay(3, settings) <= 9.2
    assert exponential_delay(8, settings) <= 16.5
    assert retry_queue_delay_ms(1, settings) in {2000, 4000, 8000, 16000}


def test_job_completes_after_retry(api):
    created = api.post(
        "/jobs",
        json={
            "type": "data_transformation",
            "payload": {"dataset": "retry-demo", "duration": 1.2, "fail_until_attempt": 3},
            "idempotency_key": "test-retry-once",
        },
    ).json()
    job = wait_status(api, created["job_id"], {"success"})
    events = [item["event"] for item in job["timeline"]]
    assert "job_retrying" in events
    assert job["attempt"] >= 3
    assert job["side_effect_committed"] is True

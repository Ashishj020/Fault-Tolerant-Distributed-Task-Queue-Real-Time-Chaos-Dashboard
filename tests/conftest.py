import os

import httpx
import pytest

API = os.environ.get("API_URL", "http://localhost:8000")


@pytest.fixture(scope="session")
def api() -> httpx.Client:
    client = httpx.Client(base_url=API, timeout=30.0)
    try:
        client.get("/health").raise_for_status()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"API is not reachable at {API}: {exc}")
    return client

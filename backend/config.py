from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://redis:6379/0"
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"

    max_retries: int = 5
    base_retry_delay: float = 2.0
    max_retry_delay: float = 16.0

    prefetch_count: int = 1
    heartbeat_interval_ms: int = 1000
    worker_id: str = "worker-local"
    worker_claim_ttl: int = 60

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: str = "http://localhost:5173,http://localhost:8080,http://localhost:8081,http://localhost:3000"

    chaos_restart_delay: int = 8
    worker_containers: str = "worker-1,worker-2,worker-3"
    results_dir: str = "/data/results"

    @property
    def cors_origin_list(self) -> list[str]:
        return [item.strip() for item in self.cors_origins.split(",") if item.strip()]

    @property
    def worker_names(self) -> list[str]:
        return [item.strip() for item in self.worker_containers.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()

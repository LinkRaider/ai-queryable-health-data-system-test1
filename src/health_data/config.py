from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "health-metrics"
    kafka_dlq_topic: str = "health-metrics-dlq"
    kafka_consumer_group: str = "health-data-ingestor-v1"
    postgres_dsn: str = "postgresql://health:health@localhost:5432/health"
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen2.5:0.5b"
    ollama_timeout_seconds: float = Field(default=60.0, ge=1, le=300)

    generator_seed: int = 20261019
    generator_user_count: int = Field(default=50, ge=1, le=10_000)
    generator_history_days: int = Field(default=7, ge=1, le=90)
    generator_live_interval_seconds: int = Field(default=30, ge=1, le=3600)

    consumer_batch_size: int = Field(default=500, ge=1, le=10_000)
    consumer_poll_timeout_ms: int = Field(default=1000, ge=100, le=30_000)
    database_pool_min_size: int = Field(default=2, ge=1, le=20)
    database_pool_max_size: int = Field(default=10, ge=1, le=100)
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from tikblog_kafka import ProducerConfig


class Settings(BaseSettings):
    """Settings for one UI or Customer Success producer."""

    model_config = SettingsConfigDict(env_prefix="EVENT_PRODUCERS_", extra="ignore")

    service_name: str = "event-producers"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    # Which source this process simulates.
    role: Literal["UI", "CUSTOMER_SUCCESS"] = "UI"

    # Kafka connection.
    bootstrap_servers: str = "localhost:9092,localhost:9093,localhost:9094"
    schema_registry_url: str = "http://localhost:8081"
    topic: str = "unsubscribe-priority"

    # Kafka batching and shutdown.
    linger_ms: int = Field(default=5, ge=0)
    batch_size: int = Field(default=16384, gt=0)
    compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = "none"
    queue_full_timeout_seconds: float = Field(default=10.0, gt=0)
    flush_timeout_seconds: float = Field(default=10.0, gt=0)

    # Generated traffic.
    events_per_second: float = Field(default=100.0, gt=0)
    user_pool_size: int = Field(default=10_000, gt=0)
    writer_pool_size: int = Field(default=500, gt=0)
    random_seed: int = 42

    # Optional bad data.
    technical_duplicate_probability: float = Field(default=0.001, ge=0.0, le=1.0)
    meaningless_event_probability: float = Field(default=0.0, ge=0.0, le=1.0)
    garbage_bytes_probability: float = Field(default=0.0, ge=0.0, le=1.0)

    @property
    def client_id(self) -> str:
        """Name used for this producer in Kafka."""
        return f"{self.service_name}-{self.role.lower()}"

    def producer_config(self) -> ProducerConfig:
        """Build the settings used by the Kafka producer."""
        return ProducerConfig(
            bootstrap_servers=self.bootstrap_servers,
            schema_registry_url=self.schema_registry_url,
            topic=self.topic,
            client_id=self.client_id,
            linger_ms=self.linger_ms,
            batch_size=self.batch_size,
            compression_type=self.compression_type,
            queue_full_timeout_seconds=self.queue_full_timeout_seconds,
            flush_timeout_seconds=self.flush_timeout_seconds,
        )

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict
from tikblog_kafka import ConsumerConfig


class Settings(BaseSettings):
    """Settings for one unsubscribe-consumer deployment."""

    model_config = SettingsConfigDict(env_prefix="UNSUBSCRIBE_CONSUMER_", extra="ignore")

    service_name: str = "unsubscribe-consumer"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    # Kafka connection. The priority and legal deployments differ only in these
    # two values, passed in through the environment.
    bootstrap_servers: str = "localhost:9092,localhost:9093,localhost:9094"
    schema_registry_url: str = "http://localhost:8081"
    topic: str = "unsubscribe-priority"
    group_id: str = "unsubscribe-priority-consumer"
    session_timeout_ms: int = 45000
    max_poll_interval_ms: int = 300000
    poll_timeout_seconds: float = 1.0

    # UnsubscribeMe HTTP client.
    unsubscribeme_url: str = "http://localhost:8080"
    http_timeout_seconds: float = 5.0
    retry_pause_seconds: float = 1.0

    def consumer_config(self) -> ConsumerConfig:
        """Build the settings used by the Kafka consumer."""
        return ConsumerConfig(
            bootstrap_servers=self.bootstrap_servers,
            schema_registry_url=self.schema_registry_url,
            topic=self.topic,
            group_id=self.group_id,
            client_id=self.service_name,
            session_timeout_ms=self.session_timeout_ms,
            max_poll_interval_ms=self.max_poll_interval_ms,
            poll_timeout_seconds=self.poll_timeout_seconds,
        )

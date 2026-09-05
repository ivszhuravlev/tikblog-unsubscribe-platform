from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from tikblog_kafka import ConsumerConfig, DlqConfig


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

    # Failure handling. A transient failure is retried in place a few times with
    # a growing pause; the budget is small on purpose, because these retries block
    # the partition. What survives the budget is parked in the dead-letter topic
    # so one bad message cannot hold up everything queued behind it.
    max_attempts: int = Field(default=3, ge=1)
    backoff_initial_seconds: float = Field(default=0.5, gt=0)
    backoff_multiplier: float = Field(default=2.0, ge=1)
    backoff_max_seconds: float = Field(default=5.0, gt=0)

    # Dead-letter topic. One per source topic, so a poisoned Legal batch can never
    # end up mixed with the priority path.
    dlq_topic: str = "unsubscribe-priority.dlq"
    dlq_flush_timeout_seconds: float = Field(default=10.0, gt=0)

    def dlq_config(self) -> DlqConfig:
        """Build the settings used by the dead-letter producer."""
        return DlqConfig(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.dlq_topic,
            client_id=f"{self.service_name}-dlq",
            flush_timeout_seconds=self.dlq_flush_timeout_seconds,
        )

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

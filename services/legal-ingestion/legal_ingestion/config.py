from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from tikblog_kafka import ProducerConfig
from tikblog_storage import StorageConfig, as_prefix


class Settings(BaseSettings):
    """Settings for one Legal ingestion run."""

    model_config = SettingsConfigDict(env_prefix="LEGAL_INGESTION_", extra="ignore")

    service_name: str = "legal-ingestion"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    # Three places inside the MinIO bucket.
    inbox_prefix: str = "legal_inbox/"
    processed_prefix: str = "legal_processed/"
    rejects_prefix: str = "legal_rejects/"

    # Local MinIO connection.
    bucket: str = "tikblog-legal"
    endpoint_url: str = "http://localhost:9000"
    access_key_id: str = "tikblog"
    secret_access_key: str = "tikblog-local"

    # Kafka connection.
    bootstrap_servers: str = "localhost:9092,localhost:9093,localhost:9094"
    schema_registry_url: str = "http://localhost:8081"
    topic: str = "unsubscribe-legal"

    # Legal files favor larger compressed batches.
    linger_ms: int = Field(default=50, ge=0)
    batch_size: int = Field(default=131072, gt=0)
    compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = "lz4"
    queue_full_timeout_seconds: float = Field(default=30.0, gt=0)
    flush_timeout_seconds: float = Field(default=30.0, gt=0)

    @model_validator(mode="after")
    def prefixes_must_be_three_different_places(self) -> "Settings":
        """Require three different, non-empty storage prefixes."""
        roles = {
            "inbox": as_prefix(self.inbox_prefix),
            "processed": as_prefix(self.processed_prefix),
            "rejects": as_prefix(self.rejects_prefix),
        }

        empty = sorted(role for role, prefix in roles.items() if not prefix)

        if empty:
            raise ValueError(f"landing zone prefixes must not be empty: {empty}")

        if len(set(roles.values())) != len(roles):
            raise ValueError(f"landing zone prefixes must differ from each other: {roles}")

        return self

    def storage_config(self) -> StorageConfig:
        """Build the settings used to connect to local MinIO."""
        return StorageConfig(
            endpoint_url=self.endpoint_url,
            bucket=self.bucket,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
        )

    def producer_config(self) -> ProducerConfig:
        """Build the settings used by the Kafka producer."""
        return ProducerConfig(
            bootstrap_servers=self.bootstrap_servers,
            schema_registry_url=self.schema_registry_url,
            topic=self.topic,
            client_id=self.service_name,
            linger_ms=self.linger_ms,
            batch_size=self.batch_size,
            compression_type=self.compression_type,
            queue_full_timeout_seconds=self.queue_full_timeout_seconds,
            flush_timeout_seconds=self.flush_timeout_seconds,
        )

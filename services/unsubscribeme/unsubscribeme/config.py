from typing import Literal

from psycopg.conninfo import make_conninfo
from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings for UnsubscribeMe: a stub of an external subscription service
    with its own OLTP database, reachable only over HTTP."""

    model_config = SettingsConfigDict(env_prefix="UNSUBSCRIBEME_", extra="ignore")

    service_name: str = "unsubscribeme"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    host: str = "0.0.0.0"
    port: int = 8080

    # This service's own Postgres, not the platform warehouse.
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "tikblog"
    postgres_user: str = "tikblog"
    postgres_password: SecretStr = SecretStr("tikblog-local")

    pool_min_size: int = Field(default=1, ge=1)
    pool_max_size: int = Field(default=10, gt=0)
    statement_timeout_ms: int = Field(default=5000, gt=0)

    # Fault injection: this stub exists to be broken on purpose in a later step.
    failure_rate: float = Field(default=0.0, ge=0, le=1)
    failure_status: int = 503
    latency_ms: int = Field(default=0, ge=0)
    # Fixes the fault-injection RNG. It pins the sequence of draws, not the
    # order request threads consume it in, so a chaos run repeats exactly
    # only when requests do not overlap.
    random_seed: int | None = None

    @model_validator(mode="after")
    def pool_max_must_not_be_below_min(self) -> "Settings":
        """A max below the min would make the pool impossible to grow to its own floor."""
        if self.pool_max_size < self.pool_min_size:
            raise ValueError(
                f"pool_max_size ({self.pool_max_size}) must be >= "
                f"pool_min_size ({self.pool_min_size})"
            )
        return self

    def dsn(self) -> str:
        """Build a libpq connection string. Never log the result: it carries the password."""
        # make_conninfo, not an f-string: libpq splits keyword=value pairs on
        # spaces, so a password from a real secrets manager either breaks the
        # string outright (a space) or is silently corrupted (a backslash is
        # eaten, and every connection then fails authentication for no visible
        # reason). Quoting is psycopg's job, not ours.
        #
        # statement_timeout travels here rather than in a pool configure
        # callback: a callback would have to open a transaction to run SET and
        # then commit it, and a connection handed back still inside a
        # transaction is one the pool discards.
        return make_conninfo(
            host=self.postgres_host,
            port=self.postgres_port,
            dbname=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            options=f"-c statement_timeout={self.statement_timeout_ms}",
        )

"""The small set of MinIO operations used by Legal ingestion."""

import io
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING, Protocol, cast

import boto3
from botocore.config import Config as BotocoreConfig

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def as_prefix(value: str) -> str:
    """Remove outer slashes and add one trailing slash."""
    return f"{value.strip('/')}/" if value.strip("/") else ""


class ObjectStore(Protocol):
    """The four MinIO operations needed by Legal ingestion."""

    def list_keys(self, prefix: str) -> list[str]: ...

    def open_stream(self, key: str) -> IO[bytes]: ...

    def write(self, key: str, data: bytes) -> None: ...

    def move(self, key: str, target_key: str) -> None: ...


class MinioObjectStore:
    """Read and move objects in one MinIO bucket."""

    def __init__(self, client: "S3Client", bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []

        for page in self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket,
            Prefix=prefix,
        ):
            keys.extend(item["Key"] for item in page.get("Contents", []))

        return sorted(keys)

    def open_stream(self, key: str) -> IO[bytes]:
        body = self._client.get_object(Bucket=self._bucket, Key=key)["Body"]
        return cast(IO[bytes], body)

    def write(self, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=self._bucket, Key=key, Body=io.BytesIO(data))

    def move(self, key: str, target_key: str) -> None:
        # MinIO has no move command: copy first, then delete the old object.
        self._client.copy_object(
            Bucket=self._bucket,
            Key=target_key,
            CopySource={"Bucket": self._bucket, "Key": key},
        )
        self._client.delete_object(Bucket=self._bucket, Key=key)


@dataclass(frozen=True, slots=True)
class StorageConfig:
    """How to connect to the local MinIO server."""

    endpoint_url: str
    bucket: str
    access_key_id: str
    secret_access_key: str


def create_object_store(config: StorageConfig) -> MinioObjectStore:
    """Connect to MinIO and return the object-storage helper."""
    client = boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        config=BotocoreConfig(s3={"addressing_style": "path"}),
    )
    return MinioObjectStore(client, config.bucket)

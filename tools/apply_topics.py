import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from confluent_kafka.admin import (  # type: ignore[attr-defined]
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewPartitions,
    NewTopic,
    ResourceType,
    TopicMetadata,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOPICS_FILE = PROJECT_ROOT / "infra/topics.yaml"

DEFAULT_BROKERS = "localhost:9092,localhost:9093,localhost:9094"
DEFAULT_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    config: dict[str, str]


def load_topic_specs() -> list[TopicSpec]:
    document = yaml.safe_load(TOPICS_FILE.read_text(encoding="utf-8"))
    raw_topics = document["topics"]

    topic_specs = []

    for raw_topic in raw_topics:
        raw_config = raw_topic.get("config", {})

        topic_specs.append(
            TopicSpec(
                name=raw_topic["name"],
                partitions=raw_topic["partitions"],
                replication_factor=raw_topic["replication_factor"],
                config={str(key): str(value) for key, value in raw_config.items()},
            )
        )

    return topic_specs


def create_topic(
    admin: AdminClient,
    topic: TopicSpec,
    timeout: float,
) -> None:
    new_topic = NewTopic(
        topic=topic.name,
        num_partitions=topic.partitions,
        replication_factor=topic.replication_factor,
        config=topic.config,
    )

    future = admin.create_topics(
        [new_topic],
        operation_timeout=timeout,
        request_timeout=timeout,
    )[topic.name]

    future.result(timeout=timeout)

    print(f"Created topic: {topic.name}")


def check_replication_factor(
    topic: TopicSpec,
    kafka_topic: TopicMetadata,
) -> None:
    partitions = kafka_topic.partitions.values()

    actual_replication_factors = {len(partition.replicas) for partition in partitions}

    expected = {topic.replication_factor}

    if actual_replication_factors != expected:
        raise RuntimeError(
            f"Topic {topic.name} has RF {actual_replication_factors}, "
            f"expected {topic.replication_factor}. "
            "Changing RF requires partition reassignment."
        )


def update_partitions(
    admin: AdminClient,
    topic: TopicSpec,
    current_count: int,
    timeout: float,
) -> bool:
    if current_count > topic.partitions:
        raise RuntimeError(
            f"Topic {topic.name} has {current_count} partitions, "
            f"but topics.yaml requests {topic.partitions}. "
            "Kafka cannot decrease partition count."
        )

    if current_count == topic.partitions:
        return False

    new_partitions = NewPartitions(
        topic=topic.name,
        new_total_count=topic.partitions,
    )

    future = admin.create_partitions(
        [new_partitions],
        operation_timeout=timeout,
        request_timeout=timeout,
    )[topic.name]

    future.result(timeout=timeout)

    print(f"Increased partitions for {topic.name}: {current_count} → {topic.partitions}")

    return True


def update_config(
    admin: AdminClient,
    topic: TopicSpec,
    timeout: float,
) -> bool:
    resource = ConfigResource(ResourceType.TOPIC, topic.name)

    current_config = admin.describe_configs(
        [resource],
        request_timeout=timeout,
    )[resource].result(timeout=timeout)

    changes = {
        key: value for key, value in topic.config.items() if current_config[key].value != value
    }

    if not changes:
        return False

    updated_resource = ConfigResource(ResourceType.TOPIC, topic.name)

    for key, value in changes.items():
        updated_resource.add_incremental_config(
            ConfigEntry(
                key,
                value,
                incremental_operation=AlterConfigOpType.SET,
            )
        )

    admin.incremental_alter_configs(
        [updated_resource],
        request_timeout=timeout,
    )[updated_resource].result(timeout=timeout)

    print(f"Updated config for {topic.name}: {changes}")

    return True


def apply_topics() -> None:
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BROKERS)
    timeout = float(
        os.getenv(
            "KAFKA_ADMIN_TIMEOUT_SECONDS",
            str(DEFAULT_TIMEOUT_SECONDS),
        )
    )

    admin = AdminClient(
        {
            "bootstrap.servers": brokers,
        }
    )

    desired_topics = load_topic_specs()
    kafka_metadata = admin.list_topics(timeout=timeout)

    for topic in desired_topics:
        kafka_topic = kafka_metadata.topics.get(topic.name)

        if kafka_topic is None:
            create_topic(admin, topic, timeout)
            continue

        if kafka_topic.error is not None:
            raise RuntimeError(f"Kafka returned an error for {topic.name}: {kafka_topic.error}")

        check_replication_factor(topic, kafka_topic)

        partitions_changed = update_partitions(
            admin,
            topic,
            current_count=len(kafka_topic.partitions),
            timeout=timeout,
        )

        config_changed = update_config(admin, topic, timeout)

        if not partitions_changed and not config_changed:
            print(f"Topic is up to date: {topic.name}")


if __name__ == "__main__":
    apply_topics()

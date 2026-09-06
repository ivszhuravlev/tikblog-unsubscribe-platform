"""Offset wake-up sensor; only its own Kafka group is committed after success."""

import asyncio

from airflow.models import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from confluent_kafka import Consumer, TopicPartition

from analytics.common.config import settings


def sensor_consumer(cfg):
    return Consumer(
        {
            "bootstrap.servers": cfg["kafka_bootstrap"],
            "group.id": cfg["sensor_group"],
            "enable.auto.commit": False,
        }
    )


def pending_offsets():
    cfg = settings()
    consumer = sensor_consumer(cfg)
    try:
        topic = cfg["legal_topic"]
        metadata = consumer.list_topics(topic, timeout=10).topics[topic]
        if metadata.error:
            raise RuntimeError(str(metadata.error))
        partitions = [TopicPartition(topic, p) for p in metadata.partitions]
        committed = consumer.committed(partitions, timeout=10)
        ends = {}
        pending = False
        for p in committed:
            low, high = consumer.get_watermark_offsets(p, timeout=10, cached=False)
            ends[str(p.partition)] = high
            start = p.offset if p.offset >= 0 else low
            pending = pending or high > start
        return {"topic": topic, "offsets": ends} if pending else None
    finally:
        consumer.close()


class LegalOffsetsTrigger(BaseTrigger):
    def serialize(self):
        return "legal_sensor.LegalOffsetsTrigger", {}

    async def run(self):
        while True:
            event = await asyncio.to_thread(pending_offsets)
            if event:
                yield TriggerEvent(event)
                return
            await asyncio.sleep(settings()["sensor_poll_seconds"])


class LegalOffsetsSensor(BaseOperator):
    def execute(self, context):
        self.defer(trigger=LegalOffsetsTrigger(), method_name="execute_complete")

    def execute_complete(self, context, event=None):
        return event


def acknowledge_sensor(**context):
    event = context["ti"].xcom_pull(task_ids="wait_for_legal_events")
    consumer = sensor_consumer(settings())
    try:
        partitions = [
            TopicPartition(event["topic"], int(p), offset) for p, offset in event["offsets"].items()
        ]
        consumer.assign(partitions)
        consumer.commit(offsets=partitions, asynchronous=False)
    finally:
        consumer.close()

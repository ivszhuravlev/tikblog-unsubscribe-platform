SELECT quarantined_at, source, event_id, failed_checks, failure_reason,
 kafka_topic, kafka_partition, kafka_offset FROM monitoring.dq_quarantine
 ORDER BY quarantined_at DESC LIMIT 100;

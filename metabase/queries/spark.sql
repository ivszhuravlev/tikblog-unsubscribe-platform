WITH latest AS (SELECT *, row_number() OVER (
 PARTITION BY pipeline_name, source, metric_name, kafka_topic, kafka_partition
 ORDER BY observed_at DESC) AS n FROM monitoring.pipeline_metrics)
SELECT pipeline_name, source, metric_name, metric_value, unit,
    status, kafka_topic, kafka_partition, observed_at FROM latest WHERE n=1 AND (metric_name IN ('microbatch_duration_ms','active_executor_count','configured_executor_count'));

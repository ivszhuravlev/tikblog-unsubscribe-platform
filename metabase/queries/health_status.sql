WITH latest AS (SELECT *, row_number() OVER (
 PARTITION BY pipeline_name, source, metric_name, kafka_topic, kafka_partition
 ORDER BY observed_at DESC) AS n FROM monitoring.pipeline_metrics)
SELECT pipeline_name, source,
 CASE max(CASE status WHEN 'RED' THEN 3 WHEN 'YELLOW' THEN 2 WHEN 'GREEN' THEN 1 ELSE 0 END)
 WHEN 3 THEN 'RED' WHEN 2 THEN 'YELLOW' WHEN 1 THEN 'GREEN' ELSE 'UNKNOWN' END AS status,
 max(observed_at) AS observed_at FROM latest WHERE n=1 GROUP BY pipeline_name, source;

SELECT created_at, pipeline_name, source, severity, metric_name, current_value,
       threshold, message FROM monitoring.alert_events ORDER BY created_at DESC LIMIT 100;

SELECT date_trunc('hour', checked_at) AS check_hour, source,
       sum(failed_rows) AS failed_check_rows
FROM monitoring.dq_results WHERE checked_at >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY date_trunc('hour', checked_at), source ORDER BY check_hour, source;

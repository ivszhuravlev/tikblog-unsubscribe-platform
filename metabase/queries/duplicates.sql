SELECT checked_at, source, check_name, failed_rows AS duplicate_events, status
 FROM monitoring.dq_results WHERE check_name LIKE 'duplicate_%' ORDER BY checked_at DESC LIMIT 100;

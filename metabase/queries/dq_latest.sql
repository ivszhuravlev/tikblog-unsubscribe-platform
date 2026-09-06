WITH latest AS (SELECT *, row_number() OVER (
 PARTITION BY pipeline_name, layer, source, check_name ORDER BY checked_at DESC) AS n
 FROM monitoring.dq_results)
 SELECT pipeline_name, layer, source, check_name, status, total_rows, failed_rows,
 failed_rate, checked_at FROM latest WHERE n=1
 ORDER BY CASE status WHEN 'RED' THEN 3 WHEN 'YELLOW' THEN 2 WHEN 'GREEN' THEN 1 ELSE 0 END DESC,
 checked_at DESC;

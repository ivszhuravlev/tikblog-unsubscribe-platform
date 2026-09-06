WITH latest AS (
    SELECT *, row_number() OVER (
        PARTITION BY pipeline_name, layer, source, check_name
        ORDER BY checked_at DESC) AS n
    FROM monitoring.dq_results
)
SELECT layer, source, sum(failed_rows) AS failed_check_rows,
       sum(CASE WHEN status = 'RED' THEN 1 ELSE 0 END) AS red_checks,
       sum(CASE WHEN status = 'YELLOW' THEN 1 ELSE 0 END) AS yellow_checks,
       max(checked_at) AS checked_at
FROM latest WHERE n = 1 GROUP BY layer, source ORDER BY layer, source;

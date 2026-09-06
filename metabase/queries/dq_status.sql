WITH latest AS (
    SELECT *, row_number() OVER (
        PARTITION BY pipeline_name, layer, source, check_name
        ORDER BY checked_at DESC) AS n
    FROM monitoring.dq_results
)
SELECT CASE max(CASE status WHEN 'RED' THEN 3 WHEN 'YELLOW' THEN 2
                           WHEN 'GREEN' THEN 1 ELSE 0 END)
           WHEN 3 THEN 'RED' WHEN 2 THEN 'YELLOW' WHEN 1 THEN 'GREEN'
           ELSE 'UNKNOWN' END AS status,
       sum(CASE WHEN status = 'RED' THEN 1 ELSE 0 END) AS red_checks,
       sum(CASE WHEN status = 'YELLOW' THEN 1 ELSE 0 END) AS yellow_checks,
       max(checked_at) AS latest_check_at
FROM latest WHERE n = 1;

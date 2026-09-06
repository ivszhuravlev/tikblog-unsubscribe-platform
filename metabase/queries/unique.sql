SELECT event_date, source, unique_readers, unique_writers FROM gold.unsubscribe_daily WHERE 1=1
[[AND event_date >= {{from_date}}]]
[[AND event_date <= {{to_date}}]]
[[AND source = {{source}}]] ORDER BY event_date DESC, source;

SELECT coalesce(sum(unsubscribe_count),0) AS requests_received FROM gold.unsubscribe_daily WHERE 1=1
[[AND event_date >= {{from_date}}]]
[[AND event_date <= {{to_date}}]]
[[AND source = {{source}}]];

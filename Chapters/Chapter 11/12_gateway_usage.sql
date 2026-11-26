SELECT destination_name, COUNT(*) AS request_count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)
         AS observed_percentage
FROM system.ai_gateway.usage
WHERE endpoint_name = 'cobalt-reliable-assistant'
  AND event_time >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY destination_name
ORDER BY request_count DESC;

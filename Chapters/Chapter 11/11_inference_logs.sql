SELECT event_time, request_id, invocation_id, requester,
       destination_name, status_code, latency_ms,
       get_json_object(request, '$.messages[0].content') AS prompt,
       get_json_object(response, '$.choices[0].message.content')
         AS response_text,
       logging_error_codes
FROM cobalt_prod.system.serving_inference_logs
WHERE event_time >= current_timestamp() - INTERVAL 1 DAY
ORDER BY event_time DESC;

-- Chapter 14 reference: Request/response forensic SQL.
-- Logged JSON only; retrieval IDs require MLflow traces.
-- Inspect actual deployed table schema before use.

-- Adjust the fully qualified table name to your configured service.
SELECT
    request_id,
    event_time,
    status_code,
    latency_ms,
    get_json_object(request, '$.messages[0].content') AS prompt,
    get_json_object(response, '$.choices[0].message.content') AS answer
FROM cobalt_prod.system_logs.qa_assistant_inference_payload
WHERE get_json_object(request, '$.messages[0].content')
      ILIKE '%XM-400%'
ORDER BY event_time DESC;

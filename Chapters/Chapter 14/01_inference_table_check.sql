-- Chapter 14 reference: Inference table configuration and coverage.
-- Unity Gateway service, governed log table; may be incomplete.
-- Inspect actual deployed table schema before use.

-- Verify the table name shown on the Unity Gateway model-service page.
SELECT request_id, event_time, status_code, latency_ms
FROM cobalt_prod.system_logs.qa_assistant_inference_payload
ORDER BY event_time DESC
LIMIT 10;

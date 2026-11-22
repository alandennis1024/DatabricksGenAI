-- Operating views for the CobaltWorks shipment-exception agent (Chapter 15).
-- Databricks SQL. The notebook replaces `cobalt_ops.future` with the catalog
-- and schema from its widgets. To run this file by hand, change the names.
-- The four views are Quality, Reliability, Economics, and Control. They read
-- the audit table from the chapter plus four monitoring tables that your
-- trace export and evaluation jobs write. The tool timeout view assumes the
-- agent writes actor_type = 'agent' for its own tool calls.
-- Unlike the chapter file, all tables and views live in one schema so the
-- sample is easy to create and clean up.

CREATE SCHEMA IF NOT EXISTS cobalt_ops.future;

-- One row per request, exported from MLflow traces.
CREATE TABLE IF NOT EXISTS cobalt_ops.future.request_traces (
  trace_id STRING,
  request_time TIMESTAMP,
  endpoint_name STRING,
  route STRING COMMENT 'lookup, analysis, or high_risk_action',
  model_name STRING,
  status STRING COMMENT 'ok, error, or timeout',
  latency_ms BIGINT,
  input_tokens BIGINT,
  cached_input_tokens BIGINT,
  output_tokens BIGINT,
  cost_usd DECIMAL(12, 6),
  task_success BOOLEAN
) USING DELTA
COMMENT 'Chapter 15 sample data: one row per request';

-- One row per scorer per trace, written by an MLflow evaluation job.
CREATE TABLE IF NOT EXISTS cobalt_ops.future.eval_scores (
  trace_id STRING,
  scored_at TIMESTAMP,
  scorer_name STRING COMMENT 'groundedness, citation_correctness, ...',
  score DOUBLE
) USING DELTA
COMMENT 'Chapter 15 sample data: one row per scorer per trace';

-- Periodic samples of serving-endpoint health.
CREATE TABLE IF NOT EXISTS cobalt_ops.future.endpoint_metrics (
  sample_time TIMESTAMP,
  endpoint_name STRING,
  queue_depth BIGINT
) USING DELTA
COMMENT 'Chapter 15 sample data: endpoint queue depth samples';

-- One row per retrieval, with the request kept as a JSON string.
CREATE TABLE IF NOT EXISTS cobalt_ops.future.retrieval_events (
  trace_id STRING,
  event_time TIMESTAMP,
  requester_tenant STRING,
  source_tenant STRING,
  chunks_returned BIGINT,
  request_json STRING
) USING DELTA
COMMENT 'Chapter 15 sample data: one row per retrieval';

-- The audit table from the chapter. action_state is expected to hold values
-- such as proposed, approval_requested, approved, policy_denied,
-- permission_denied, tool_timeout, tool_error, and committed.
CREATE TABLE IF NOT EXISTS cobalt_ops.future.agent_actions (
  event_id STRING,
  trace_id STRING,
  event_time TIMESTAMP,
  actor_type STRING,
  actor_id STRING,
  tool_name STRING,
  action_state STRING,
  policy_version STRING,
  source_refs ARRAY<STRING>,
  idempotency_key STRING,
  redaction_status STRING
) USING DELTA
COMMENT 'Chapter 15 sample data: agent audit events';

-- 1. Quality: groundedness, citation correctness, refusal accuracy, and
--    task success. Question: are shipment answers still citing the right
--    carrier record?
CREATE OR REPLACE VIEW cobalt_ops.future.v_quality_daily AS
SELECT
  CAST(s.scored_at AS DATE) AS score_date,
  s.scorer_name,
  avg(s.score) AS avg_score,
  count(*) AS scored_traces,
  count_if(s.score < 0.7) AS low_score_traces
FROM cobalt_ops.future.eval_scores AS s
GROUP BY CAST(s.scored_at AS DATE), s.scorer_name;

CREATE OR REPLACE VIEW cobalt_ops.future.v_task_success_daily AS
SELECT
  CAST(t.request_time AS DATE) AS request_date,
  count(*) AS requests,
  count_if(t.task_success) AS successful_tasks,
  count_if(t.task_success) / count(*) AS task_success_rate
FROM cobalt_ops.future.request_traces AS t
GROUP BY CAST(t.request_time AS DATE);

-- Alert query: a three-day rolling groundedness decline opens an
-- investigation with example traces.
SELECT
  score_date,
  avg_score,
  avg(avg_score) OVER (
    ORDER BY score_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS rolling_3_day_avg
FROM cobalt_ops.future.v_quality_daily
WHERE scorer_name = 'groundedness'
ORDER BY score_date DESC
LIMIT 14;

-- 2. Reliability: endpoint errors, tool timeouts, queue depth, and latency.
--    Question: is the shipment MCP tool timing out during the morning rush?
CREATE OR REPLACE VIEW cobalt_ops.future.v_endpoint_reliability_daily AS
SELECT
  CAST(t.request_time AS DATE) AS request_date,
  t.endpoint_name,
  count(*) AS requests,
  count_if(t.status <> 'ok') / count(*) AS error_rate,
  percentile_approx(t.latency_ms, 0.95) AS p95_latency_ms
FROM cobalt_ops.future.request_traces AS t
GROUP BY CAST(t.request_time AS DATE), t.endpoint_name;

CREATE OR REPLACE VIEW cobalt_ops.future.v_tool_timeouts_hourly AS
SELECT
  date_trunc('HOUR', a.event_time) AS event_hour,
  a.tool_name,
  count(*) AS tool_calls,
  count_if(a.action_state = 'tool_timeout') AS timeouts,
  count_if(a.action_state = 'tool_timeout') / count(*) AS timeout_rate
FROM cobalt_ops.future.agent_actions AS a
WHERE a.actor_type = 'agent'
GROUP BY date_trunc('HOUR', a.event_time), a.tool_name;

CREATE OR REPLACE VIEW cobalt_ops.future.v_queue_depth_hourly AS
SELECT
  date_trunc('HOUR', m.sample_time) AS sample_hour,
  m.endpoint_name,
  max(m.queue_depth) AS max_queue_depth
FROM cobalt_ops.future.endpoint_metrics AS m
GROUP BY date_trunc('HOUR', m.sample_time), m.endpoint_name;

-- 3. Economics: tokens, cache hit rate, compute cost, and cost per
--    successful task. Question: is a rebooking request costing more than it
--    did last month?
CREATE OR REPLACE VIEW cobalt_ops.future.v_economics_daily AS
SELECT
  CAST(t.request_time AS DATE) AS request_date,
  t.route,
  sum(t.input_tokens + t.output_tokens) AS total_tokens,
  sum(t.cached_input_tokens) / nullif(sum(t.input_tokens), 0)
    AS cache_hit_rate,
  sum(t.cost_usd) AS total_cost_usd,
  count_if(t.task_success) AS successful_tasks,
  sum(t.cost_usd) / nullif(count_if(t.task_success), 0)
    AS cost_per_successful_task_usd
FROM cobalt_ops.future.request_traces AS t
GROUP BY CAST(t.request_time AS DATE), t.route;

-- Month-over-month cost per successful task, by route.
SELECT
  route,
  date_trunc('MONTH', request_date) AS month,
  sum(total_cost_usd) / nullif(sum(successful_tasks), 0) AS cost_per_success
FROM cobalt_ops.future.v_economics_daily
GROUP BY route, date_trunc('MONTH', request_date)
ORDER BY route, month DESC;

-- 4. Control: approval rates, policy denials, permission failures, and
--    suspicious retrieval patterns. Question: how often does the policy
--    service hold a rebooking for approval?
CREATE OR REPLACE VIEW cobalt_ops.future.v_control_daily AS
SELECT
  CAST(a.event_time AS DATE) AS event_date,
  a.tool_name,
  count_if(a.action_state = 'proposed') AS proposals,
  count_if(a.action_state = 'approval_requested') AS held_for_approval,
  count_if(a.action_state = 'approved') AS approvals,
  count_if(a.action_state = 'policy_denied') AS policy_denials,
  count_if(a.action_state = 'permission_denied') AS permission_failures,
  count_if(a.action_state = 'approval_requested')
    / nullif(count_if(a.action_state = 'proposed'), 0) AS approval_hold_rate
FROM cobalt_ops.future.agent_actions AS a
GROUP BY CAST(a.event_time AS DATE), a.tool_name;

-- Suspicious retrieval: cross-tenant reads and bursts of repeated retrieval.
-- The ':' operator reads a field from the JSON string column.
CREATE OR REPLACE VIEW cobalt_ops.future.v_suspicious_retrieval AS
WITH bursts AS (
  SELECT e.trace_id
  FROM cobalt_ops.future.retrieval_events AS e
  GROUP BY e.trace_id
  HAVING count(*) > 50
)
SELECT
  r.trace_id,
  r.event_time,
  r.requester_tenant,
  r.source_tenant,
  r.request_json:query_type::STRING AS query_type,
  CASE
    WHEN r.requester_tenant <> r.source_tenant THEN 'cross_tenant'
    ELSE 'burst'
  END AS pattern
FROM cobalt_ops.future.retrieval_events AS r
LEFT JOIN bursts AS b ON r.trace_id = b.trace_id
WHERE r.requester_tenant <> r.source_tenant
  OR b.trace_id IS NOT NULL;

-- Alert query: a rising approval hold rate may mean that the router is
-- sending routine work to a high-risk path.
SELECT
  event_date,
  held_for_approval,
  proposals,
  approval_hold_rate
FROM cobalt_ops.future.v_control_daily
WHERE tool_name = 'propose_rebooking'
ORDER BY event_date DESC
LIMIT 14;

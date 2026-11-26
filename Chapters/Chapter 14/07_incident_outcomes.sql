-- Chapter 14 reference: Observational downtime comparison.
-- Requires reviewed, incident-level cohort dataset.
-- Inspect actual deployed table schema before use.

-- Requires a reviewed incident-level cohort table with one row/incident.
-- Assistance alone does not establish a causal effect.
SELECT
    assistance_group,
    count(*) AS incidents,
    avg(downtime_minutes) AS avg_downtime_minutes,
    sum(downtime_minutes * cost_per_minute_usd)
        AS observed_downtime_cost_usd
FROM cobalt_prod.analytics_gold.incident_outcomes_reviewed
WHERE incident_type = 'ROBOTIC_PRESS_JAM'
GROUP BY assistance_group;

-- Apply a sensitivity tag to the HR resume screening agent
ALTER MODEL cobalt_prod.human_resources.resume_screening_agent 
SET TAGS ('sensitivity' = 'PII_HIGH', 'department' = 'HR_ONLY');

-- Apply an export control tag to a proprietary engineering model
ALTER MODEL cobalt_prod.engineering.turbine_stress_simulator
SET TAGS ('export_control' = 'ITAR_RESTRICTED');

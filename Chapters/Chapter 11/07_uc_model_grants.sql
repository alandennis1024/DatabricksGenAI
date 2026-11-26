-- Prerequisite: account group and service principal already exist.
GRANT USE CATALOG ON CATALOG cobalt_prod TO `qa_ml_engineers`;
GRANT USE SCHEMA ON SCHEMA cobalt_prod.quality_assurance
TO `qa_ml_engineers`;

GRANT CREATE MODEL ON SCHEMA cobalt_prod.quality_assurance
TO `qa_ml_engineers`;

-- Registering model versions and deploying require additional rights.
GRANT USE CATALOG ON CATALOG cobalt_prod TO `sp_qa_prod_app`;
GRANT USE SCHEMA ON SCHEMA cobalt_prod.quality_assurance
TO `sp_qa_prod_app`;
GRANT EXECUTE ON MODEL cobalt_prod.quality_assurance.defect_detector
TO `sp_qa_prod_app`;

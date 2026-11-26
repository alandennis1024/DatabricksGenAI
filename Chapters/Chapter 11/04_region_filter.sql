-- Create a mapping table that defines which users can see which regions
CREATE TABLE cobalt_prod.security.region_access_map (
  user_email STRING,
  allowed_region STRING
);

INSERT INTO cobalt_prod.security.region_access_map VALUES
  ('hans.muller@cobaltworks.com', 'EU'),
  ('sarah.connor@cobaltworks.com', 'NA');

-- Create a filter function
CREATE OR REPLACE FUNCTION cobalt_prod.security.region_filter(
  region STRING
) RETURNS BOOLEAN
RETURN EXISTS (
  SELECT 1 FROM cobalt_prod.security.region_access_map
  WHERE user_email = CURRENT_USER() AND allowed_region = region
) OR IS_ACCOUNT_GROUP_MEMBER('global_admins');

-- Apply the filter to the silver training table
ALTER TABLE cobalt_prod.factory_silver.turbine_telemetry
SET ROW FILTER cobalt_prod.security.region_filter ON (factory_region);

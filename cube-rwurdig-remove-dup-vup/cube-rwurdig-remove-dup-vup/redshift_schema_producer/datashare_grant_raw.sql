-- Grant CREATE, ALTER, DROP on external schema (not SELECT)
GRANT CREATE, ALTER, DROP ON EXTERNAL SCHEMA "raw" TO DATASHARE vector_core_datashare;

-- For external tables, we grant schema-level permissions instead of individual table grants
-- This avoids the Lake Formation permission issues
GRANT USAGE ON SCHEMA "raw" TO DATASHARE vector_core_datashare;

-- Note: External tables in the "raw" schema are already included via:
-- ALTER DATASHARE vector_core_datashare ADD ALL TABLES IN SCHEMA "raw";
-- in the main DATASHARE_PRODUCER.sql file
-- Within the namespace we want to consume data from (vector-api-namespace)
CREATE DATABASE core WITH PERMISSIONS FROM DATASHARE vector_core_datashare OF account '867543562762' NAMESPACE 'a6d274df-afa7-41f1-a926-8f070c853847';
CREATE DATABASE vectordb WITH PERMISSIONS FROM DATASHARE vector_db_datashare OF account '867543562762' NAMESPACE 'a6d274df-afa7-41f1-a926-8f070c853847';

-- Create external schema from the datashare database
-- This follows the AWS article's guidance for external schemas in datashares
CREATE EXTERNAL SCHEMA raw
FROM REDSHIFT DATABASE core
SCHEMA raw;

-- Grant usage permissions on the external schema
-- This is required for users to access external tables
GRANT USAGE ON SCHEMA raw TO PUBLIC;
GRANT USAGE ON SCHEMA core.raw TO PUBLIC; 
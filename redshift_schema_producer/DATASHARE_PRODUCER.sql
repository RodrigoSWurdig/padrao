-- Within the namespace we want to originate data from (vector-namespace)

-- Base Datashare for our core data
CREATE DATASHARE "vector_core_datashare";
ALTER DATASHARE vector_core_datashare SET PUBLICACCESSIBLE TRUE;

ALTER DATASHARE vector_core_datashare ADD SCHEMA census;
ALTER DATASHARE vector_core_datashare ADD ALL TABLES IN SCHEMA census;
ALTER DATASHARE vector_core_datashare ADD ALL FUNCTIONS IN SCHEMA census;
ALTER DATASHARE vector_core_datashare SET INCLUDENEW TRUE FOR SCHEMA census;

ALTER DATASHARE vector_core_datashare ADD SCHEMA prod_pre_aggregations;
ALTER DATASHARE vector_core_datashare ADD ALL TABLES IN SCHEMA prod_pre_aggregations;
ALTER DATASHARE vector_core_datashare ADD ALL FUNCTIONS IN SCHEMA prod_pre_aggregations;
ALTER DATASHARE vector_core_datashare SET INCLUDENEW TRUE FOR SCHEMA prod_pre_aggregations;

ALTER DATASHARE vector_core_datashare ADD SCHEMA base;
ALTER DATASHARE vector_core_datashare ADD ALL TABLES IN SCHEMA base;
ALTER DATASHARE vector_core_datashare ADD ALL FUNCTIONS IN SCHEMA base;
ALTER DATASHARE vector_core_datashare SET INCLUDENEW TRUE FOR SCHEMA base;

ALTER DATASHARE vector_core_datashare ADD SCHEMA derived;
ALTER DATASHARE vector_core_datashare ADD ALL TABLES IN SCHEMA derived;
ALTER DATASHARE vector_core_datashare ADD ALL FUNCTIONS IN SCHEMA derived;
ALTER DATASHARE vector_core_datashare SET INCLUDENEW TRUE FOR SCHEMA derived;

ALTER DATASHARE vector_core_datashare ADD SCHEMA "raw";
ALTER DATASHARE vector_core_datashare ADD ALL TABLES IN SCHEMA "raw";
ALTER DATASHARE vector_core_datashare ADD ALL FUNCTIONS IN SCHEMA "raw";
ALTER DATASHARE vector_core_datashare SET INCLUDENEW TRUE FOR SCHEMA "raw";

-- Vector DB Datashare
CREATE DATASHARE "vector_db_datashare";
ALTER DATASHARE vector_db_datashare SET PUBLICACCESSIBLE TRUE;
ALTER DATASHARE vector_db_datashare ADD SCHEMA public;
ALTER DATASHARE vector_db_datashare ADD ALL TABLES IN SCHEMA public;
ALTER DATASHARE vector_db_datashare ADD ALL FUNCTIONS IN SCHEMA public;
ALTER DATASHARE vector_db_datashare SET INCLUDENEW TRUE FOR SCHEMA public;


-- Segment Evaluation Usage Grants
GRANT USAGE ON DATASHARE vector_core_datashare TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON SCHEMA census TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON SCHEMA prod_pre_aggregations TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON SCHEMA base TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON SCHEMA derived TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON SCHEMA 'raw' TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON ALL TABLES IN SCHEMA census TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON ALL TABLES IN SCHEMA prod_pre_aggregations TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON ALL TABLES IN SCHEMA base TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON ALL TABLES IN SCHEMA derived TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON ALL TABLES IN SCHEMA 'raw' TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';

GRANT USAGE ON DATASHARE vector_db_datashare TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON SCHEMA public TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
GRANT ALL ON ALL TABLES IN SCHEMA public TO NAMESPACE 'a4dd6eb0-5914-43ed-aa29-f80da082673c';
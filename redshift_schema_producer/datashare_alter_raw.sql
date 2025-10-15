ALTER DATASHARE vector_core_datashare ADD SCHEMA raw;

-- Add each external table to the datashare
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".device_360;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".email_optout;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".email_validation;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".firmographic;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".full_email_list;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".ip_to_company;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".ip_to_hem;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".live_intent;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".maid_to_hem;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".maid_to_ip;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".mappings;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".market_pulse;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".market_pulse_2;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".market_pulse_custom_topic;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".market_pulse_custom_topics_taxonomy;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".market_pulse_taxonomy;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".pdl_job_title_level;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".pdl_person;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".resolutions;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".universal_person;
ALTER DATASHARE vector_core_datashare ADD TABLE "raw".up_to_hem;
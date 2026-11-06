CREATE CATALOG IF NOT EXISTS bank_dev COMMENT 'Banking lakehouse - dev';

USE CATALOG bank_dev;   -- or: USE CATALOG workspace;

CREATE SCHEMA IF NOT EXISTS raw    COMMENT 'Landing zone for source files';
CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw ingested tables; clear-text PII, restricted';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Typed, validated data; PII protected by column masks';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Business aggregates for BI';

CREATE VOLUME IF NOT EXISTS raw.landing COMMENT 'Synthetic banking files land here';

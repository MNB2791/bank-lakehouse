-- Production design: run when these account-level groups exist.
USE CATALOG bank_dev;

GRANT ALL PRIVILEGES ON SCHEMA raw    TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA silver TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA gold   TO `data-engineers`;

-- Bronze stays closed to everyone else: it holds clear-text SSNs
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `compliance`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `compliance`;
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `region-northeast`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `region-southeast`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `region-midwest`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold   TO `region-west`;
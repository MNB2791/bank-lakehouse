USE CATALOG bank_dev;

-- Column masks: compliance sees clear text, everyone else sees a redacted value of the same type
CREATE OR REPLACE FUNCTION silver.mask_text(v STRING)
  RETURN CASE WHEN is_account_group_member('compliance') THEN v ELSE '*** REDACTED ***' END;

CREATE OR REPLACE FUNCTION silver.mask_email(v STRING)
  RETURN CASE WHEN is_account_group_member('compliance') THEN v
              ELSE concat('***@', substring_index(v, '@', -1)) END;

CREATE OR REPLACE FUNCTION silver.mask_phone(v STRING)
  RETURN CASE WHEN is_account_group_member('compliance') THEN v
              ELSE concat('***-***-', right(regexp_replace(v, '[^0-9]', ''), 4)) END;

CREATE OR REPLACE FUNCTION silver.mask_dob(v DATE)       -- generalize to birth year
  RETURN CASE WHEN is_account_group_member('compliance') THEN v
              ELSE make_date(year(v), 1, 1) END;

ALTER STREAMING TABLE silver.customers ALTER COLUMN first_name    SET MASK silver.mask_text;
ALTER STREAMING TABLE silver.customers ALTER COLUMN last_name     SET MASK silver.mask_text;
ALTER STREAMING TABLE silver.customers ALTER COLUMN email         SET MASK silver.mask_email;
ALTER STREAMING TABLE silver.customers ALTER COLUMN phone         SET MASK silver.mask_phone;
ALTER STREAMING TABLE silver.customers ALTER COLUMN date_of_birth SET MASK silver.mask_dob;

-- Tag PII columns so they are discoverable in Catalog Explorer
ALTER STREAMING TABLE silver.customers ALTER COLUMN email SET TAGS ('pii' = 'email');
ALTER STREAMING TABLE silver.customers ALTER COLUMN phone SET TAGS ('pii' = 'phone');

-- Row filter: regional managers see only their own region's Gold rows.
-- Workspace admins and the engineering/compliance/analyst groups see all rows.
CREATE OR REPLACE FUNCTION gold.region_filter(r STRING)
  RETURN is_member('admins')
      OR is_account_group_member('data-engineers')
      OR is_account_group_member('compliance')
      OR is_account_group_member('analysts')
      OR (r = 'Northeast' AND is_account_group_member('region-northeast'))
      OR (r = 'Southeast' AND is_account_group_member('region-southeast'))
      OR (r = 'Midwest'   AND is_account_group_member('region-midwest'))
      OR (r = 'West'      AND is_account_group_member('region-west'));

ALTER MATERIALIZED VIEW gold.daily_branch_txn_volume SET ROW FILTER gold.region_filter ON (region);

-- 1. Masks work: expect REDACTED names, ***@domain emails, ***-***-1234 phones, DOB as Jan 1 of the birth year
SELECT customer_id, first_name, last_name, email, phone, date_of_birth, ssn_masked, ssn_hash
FROM bank_dev.silver.customers LIMIT 5;

-- 2. Row filter does not lock you out: should still be about 3,640 rows
SELECT COUNT(*) FROM bank_dev.gold.daily_branch_txn_volume;

-- 3. Gold still reconciles (masks must not affect aggregates): should be 908,182
SELECT SUM(txn_count) FROM bank_dev.gold.daily_branch_txn_volume;

-- 4. The bad-case proof: show the masks are attached to the columns
DESCRIBE EXTENDED bank_dev.silver.customers;
-- 1. Daily branch transaction volume ------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bank_dev.gold.daily_branch_txn_volume
COMMENT 'Transaction count, value, flows and fraud rate by branch and day'
AS
SELECT
  t.txn_date,
  b.branch_id,
  b.branch_name,
  b.region,
  b.state,
  COUNT(*)                                                    AS txn_count,
  COUNT(DISTINCT t.account_id)                                AS active_accounts,
  SUM(t.amount)                                               AS total_amount,
  SUM(CASE WHEN t.direction = 'IN'  THEN t.amount ELSE 0 END) AS inflow,
  SUM(CASE WHEN t.direction = 'OUT' THEN t.amount ELSE 0 END) AS outflow,
  SUM(t.fraud_flag)                                           AS fraud_txn_count,
  ROUND(100.0 * SUM(t.fraud_flag) / COUNT(*), 3)              AS fraud_rate_pct
FROM bank_dev.silver.transactions t
JOIN bank_dev.silver.accounts a ON t.account_id = a.account_id
JOIN bank_dev.silver.branches b ON a.branch_id  = b.branch_id
GROUP BY ALL;


-- 2. High-risk customer summary -----------------------------------------------
-- Transparent, tunable rule-based score (0-100).
CREATE OR REFRESH MATERIALIZED VIEW bank_dev.gold.high_risk_customers
COMMENT 'Customer risk score from fraud flags, high-value and foreign activity, KYC and credit'
AS
WITH txn_agg AS (
  SELECT
    a.customer_id,
    COUNT(*)                                          AS txn_count,
    SUM(t.amount)                                     AS total_amount,
    SUM(t.fraud_flag)                                 AS fraud_flag_count,
    SUM(CASE WHEN t.is_high_value THEN 1 ELSE 0 END)  AS high_value_count,
    SUM(CASE WHEN t.is_foreign    THEN 1 ELSE 0 END)  AS foreign_count,
    MAX(t.txn_ts)                                     AS last_txn_ts
  FROM bank_dev.silver.transactions t
  JOIN bank_dev.silver.accounts a ON t.account_id = a.account_id
  GROUP BY a.customer_id
),
scored AS (
  SELECT
    c.customer_id, c.segment, c.state, c.age_band, c.credit_score, c.kyc_status,
    x.txn_count, x.total_amount, x.fraud_flag_count, x.high_value_count, x.foreign_count, x.last_txn_ts,
    LEAST(100,
        25 * x.fraud_flag_count
      +  5 * x.high_value_count
      +  1 * x.foreign_count
      + CASE WHEN c.kyc_status <> 'verified' THEN 15 ELSE 0 END
      + CASE WHEN c.credit_score < 580      THEN 10 ELSE 0 END
    ) AS risk_score
  FROM bank_dev.silver.customers c
  JOIN txn_agg x ON c.customer_id = x.customer_id
)
SELECT *,
  CASE WHEN risk_score >= 60 THEN 'HIGH'
       WHEN risk_score >= 30 THEN 'MEDIUM'
       ELSE 'LOW' END AS risk_tier
FROM scored;


-- 3. Monthly account balance trend --------------------------------------------
-- Month-end balance = opening balance + cumulative signed transaction amounts.
CREATE OR REFRESH MATERIALIZED VIEW bank_dev.gold.monthly_account_balance
COMMENT 'Month-end balance and net change per account'
AS
WITH monthly AS (
  SELECT
    account_id,
    date_trunc('MONTH', txn_date) AS month_start,
    SUM(signed_amount)            AS net_change,
    COUNT(*)                      AS txn_count
  FROM bank_dev.silver.transactions
  GROUP BY account_id, date_trunc('MONTH', txn_date)
)
SELECT
  m.account_id,
  a.account_type,
  a.branch_id,
  m.month_start,
  m.net_change,
  m.txn_count,
  a.opening_balance
    + SUM(m.net_change) OVER (PARTITION BY m.account_id ORDER BY m.month_start) AS month_end_balance
FROM monthly m
JOIN bank_dev.silver.accounts a ON m.account_id = a.account_id;
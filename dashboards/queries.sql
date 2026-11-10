-- Bank Operations & Risk dashboard: datasets
-- Each query reads only Gold tables in the bank_dev catalog.

-- Dataset 1: branch_performance (last 30 days vs the 30 days before)
WITH anchor AS (
  SELECT MAX(txn_date) AS d FROM bank_dev.gold.daily_branch_txn_volume
),
tagged AS (
  SELECT v.*, CASE WHEN v.txn_date > date_sub(a.d, 30) THEN 'cur' ELSE 'prior' END AS period
  FROM bank_dev.gold.daily_branch_txn_volume v
  CROSS JOIN anchor a
  WHERE v.txn_date > date_sub(a.d, 60)
)
SELECT
  branch_id, branch_name, region,
  SUM(CASE WHEN period = 'cur' THEN txn_count END)    AS txn_count_30d,
  SUM(CASE WHEN period = 'cur' THEN total_amount END) AS txn_value_30d,
  SUM(CASE WHEN period = 'cur' THEN fraud_txn_count END) AS fraud_txns_30d,
  ROUND(100.0 * (SUM(CASE WHEN period = 'cur' THEN total_amount END)
        / NULLIF(SUM(CASE WHEN period = 'prior' THEN total_amount END), 0) - 1), 1) AS value_change_pct,
  ROUND(100.0 * SUM(CASE WHEN period = 'cur' THEN fraud_txn_count END)
        / NULLIF(SUM(CASE WHEN period = 'cur' THEN txn_count END), 0), 3)          AS fraud_rate_pct_30d
FROM tagged
GROUP BY branch_id, branch_name, region
ORDER BY txn_value_30d DESC;

-- Dataset 2: risk_tier_mix
SELECT risk_tier, segment,
       COUNT(*)                   AS customers,
       ROUND(AVG(risk_score), 1)  AS avg_risk_score,
       SUM(fraud_flag_count)      AS fraud_flags,
       SUM(total_amount)          AS total_exposure
FROM bank_dev.gold.high_risk_customers
GROUP BY risk_tier, segment;

-- Dataset 3: risk_watchlist
SELECT customer_id, segment, state, risk_score, fraud_flag_count,
       high_value_count, foreign_count, kyc_status, credit_score, last_txn_ts
FROM bank_dev.gold.high_risk_customers
WHERE risk_tier = 'HIGH'
ORDER BY risk_score DESC, fraud_flag_count DESC
LIMIT 50;

-- Dataset 4: monthly_balance
SELECT month_start,
       account_type,
       SUM(month_end_balance)                                        AS total_balance,
       SUM(net_flow)                                                 AS net_flow,
       COUNT(DISTINCT account_id)                                    AS accounts,
       ROUND(SUM(month_end_balance) / COUNT(DISTINCT account_id), 2) AS avg_balance
FROM bank_dev.gold.monthly_account_balance
GROUP BY month_start, account_type
ORDER BY month_start;
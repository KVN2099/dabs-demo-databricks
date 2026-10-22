-- Gold: monthly spend per merchant. Aggregated, no PII.
CREATE OR REFRESH MATERIALIZED VIEW merchant_spend_monthly (
  CONSTRAINT has_merchant EXPECT (merchant IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Monthly card spend and average ticket by merchant'
AS SELECT
  merchant,
  date_trunc('MONTH', txn_ts) AS month,
  COUNT(*) AS txn_count,
  SUM(amount) AS total_amount,
  ROUND(AVG(amount), 2) AS avg_ticket
FROM ${source_catalog}.${silver_schema}.transactions
GROUP BY merchant, date_trunc('MONTH', txn_ts);

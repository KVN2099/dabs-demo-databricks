-- Gold: daily spend per region and category. Aggregated, no PII.
CREATE OR REFRESH MATERIALIZED VIEW daily_spend_by_region (
  CONSTRAINT non_negative_spend EXPECT (total_amount >= 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'Daily card spend by region and category'
AS SELECT
  region,
  CAST(txn_ts AS DATE) AS txn_date,
  category,
  COUNT(*) AS txn_count,
  SUM(amount) AS total_amount
FROM ${source_catalog}.${silver_schema}.transactions
GROUP BY region, CAST(txn_ts AS DATE), category;

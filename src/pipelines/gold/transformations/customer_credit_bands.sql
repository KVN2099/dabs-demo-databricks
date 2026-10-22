-- Gold: customer counts per region and credit band. Aggregated, no PII.
CREATE OR REFRESH MATERIALIZED VIEW customer_credit_bands (
  CONSTRAINT known_band EXPECT (credit_band IS NOT NULL)
)
COMMENT 'Customer counts by region and credit score band'
AS SELECT
  region,
  CASE
    WHEN credit_score >= 740 THEN 'excellent'
    WHEN credit_score >= 670 THEN 'good'
    WHEN credit_score >= 580 THEN 'fair'
    ELSE 'poor'
  END AS credit_band,
  COUNT(*) AS customers
FROM ${source_catalog}.${silver_schema}.customers
GROUP BY ALL;

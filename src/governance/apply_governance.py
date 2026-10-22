# Databricks notebook source
# MAGIC %md
# MAGIC # Apply governance
# MAGIC
# MAGIC Runs first in every environment so tables are born governed. Idempotent.
# MAGIC
# MAGIC | Control | Object | Behavior |
# MAGIC |---|---|---|
# MAGIC | Entitlements table | `governance.entitlements` | Who sees which regions and who may see clear PII |
# MAGIC | Row filter | `silver.customers`, `silver.transactions` | Rows limited to the caller's entitled regions |
# MAGIC | Column mask | `silver.customers.email` | Partially masked unless the caller has `pii_clear` |
# MAGIC | ABAC policies | `silver` schema | Any column tagged `class.us_ssn`, `class.phone_number` or `class.credit_card` is masked for everyone except the env service principal and approved PII readers |

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
governance_schema = dbutils.widgets.get("governance_schema")
env_sp = dbutils.widgets.get("env_sp")
analyst = dbutils.widgets.get("analyst")
analyst_regions = [r.strip() for r in dbutils.widgets.get("analyst_regions").split(",") if r.strip()]
pii_readers = [p.strip() for p in dbutils.widgets.get("pii_readers").split(",") if p.strip()]

gov = f"`{catalog}`.`{governance_schema}`"
silver = f"`{catalog}`.`{silver_schema}`"


def run(statement: str) -> None:
    print(statement.strip(), end="\n\n")
    spark.sql(statement)

# COMMAND ----------

# MAGIC %md ## 1. Entitlements table

# COMMAND ----------

run(f"""
CREATE TABLE IF NOT EXISTS {gov}.entitlements (
  principal STRING COMMENT 'User email or service principal application ID',
  region    STRING COMMENT 'Region the principal may see, or ALL',
  pii_clear BOOLEAN COMMENT 'True if the principal may see clear email addresses'
)
COMMENT 'Drives the region row filter and the email column mask'
""")

rows = [(env_sp, "ALL", True)]
rows += [(analyst, region, analyst in pii_readers) for region in analyst_regions]
rows += [(p, "NONE", True) for p in pii_readers if p not in (analyst, env_sp)]

(
    spark.createDataFrame(rows, "principal STRING, region STRING, pii_clear BOOLEAN")
    .write.mode("overwrite")
    .saveAsTable(f"{catalog}.{governance_schema}.entitlements")
)
display(spark.table(f"{catalog}.{governance_schema}.entitlements"))

# COMMAND ----------

# MAGIC %md ## 2. Row filter and column mask functions (table-level controls)

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {gov}.region_filter(row_region STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: caller sees only regions granted in governance.entitlements'
RETURN EXISTS (
  SELECT 1 FROM {gov}.entitlements e
  WHERE e.principal = current_user()
    AND (e.region = 'ALL' OR e.region = row_region)
)
""")

run(f"""
CREATE OR REPLACE FUNCTION {gov}.mask_email(email STRING)
RETURNS STRING
COMMENT 'Column mask: j***@example.com unless the caller has pii_clear'
RETURN CASE
  WHEN EXISTS (
    SELECT 1 FROM {gov}.entitlements e
    WHERE e.principal = current_user() AND e.pii_clear
  ) THEN email
  ELSE concat(left(email, 1), '***@', element_at(split(email, '@'), 2))
END
""")

# COMMAND ----------

# MAGIC %md ## 3. ABAC mask functions (attached by tag, not by table)

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {gov}.mask_keep_last4(value STRING)
RETURNS STRING
COMMENT 'ABAC mask for PII identifiers: keep the last 4 characters'
RETURN concat(repeat('*', greatest(length(value) - 4, 0)), right(value, 4))
""")

run(f"""
CREATE OR REPLACE FUNCTION {gov}.mask_card_number(card STRING)
RETURNS STRING
COMMENT 'ABAC mask for PCI card numbers: **** **** **** 1234'
RETURN concat('**** **** **** ', right(card, 4))
""")

# COMMAND ----------

# MAGIC %md ## 4. Silver tables, classified with governed tags and protected at creation

# COMMAND ----------

run(f"""
CREATE TABLE IF NOT EXISTS {silver}.customers (
  customer_id   BIGINT,
  full_name     STRING,
  email         STRING,
  phone         STRING,
  ssn           STRING,
  date_of_birth DATE,
  region        STRING,
  credit_score  INT,
  created_at    TIMESTAMP
)
COMMENT 'Curated customers. PII protected by row filter, column mask and ABAC.'
""")

run(f"""
CREATE TABLE IF NOT EXISTS {silver}.transactions (
  txn_id      BIGINT,
  customer_id BIGINT,
  card_number STRING,
  merchant    STRING,
  category    STRING,
  amount      DECIMAL(12, 2),
  region      STRING,
  txn_ts      TIMESTAMP
)
COMMENT 'Curated card transactions. PCI protected by ABAC.'
""")

# Only tags that drive ABAC policies. Applying a governed tag needs the
# tagPolicy.assigner role on that tag for the env service principal.
column_tags = {
    "customers": {
        "phone": "class.phone_number",
        "ssn": "class.us_ssn",
    },
    "transactions": {
        "card_number": "class.credit_card",
    },
}
for table, tags in column_tags.items():
    for column, tag in tags.items():
        run(f"ALTER TABLE {silver}.{table} ALTER COLUMN {column} SET TAGS ('{tag}')")

for table in ("customers", "transactions"):
    run(f"ALTER TABLE {silver}.{table} SET ROW FILTER {gov}.region_filter ON (region)")
run(f"ALTER TABLE {silver}.customers ALTER COLUMN email SET MASK {gov}.mask_email")

# COMMAND ----------

# MAGIC %md ## 5. ABAC policies: one rule per data class, applied to every table in the schema

# COMMAND ----------

exempt = ", ".join(f"`{p}`" for p in [env_sp, *pii_readers])

abac_policies = {
    "pii_mask_ssn": ("class.us_ssn", "mask_keep_last4", "PII: SSN shows last 4 only"),
    "pii_mask_phone": ("class.phone_number", "mask_keep_last4", "PII: phone shows last 4 only"),
    "pci_mask_card_number": ("class.credit_card", "mask_card_number", "PCI: card number shows last 4 only"),
}
for policy, (tag, function, comment) in abac_policies.items():
    run(f"""
    CREATE OR REPLACE POLICY {policy}
    ON SCHEMA {silver}
    COMMENT '{comment}'
    COLUMN MASK {gov}.{function}
    TO `account users`
    EXCEPT {exempt}
    FOR TABLES
    MATCH COLUMNS has_tag('{tag}') AS col
    ON COLUMN col
    """)

display(spark.sql(f"SHOW POLICIES ON SCHEMA {silver}"))

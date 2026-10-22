# DABs demo: environments, CI/CD and lower-environment data protection

A Databricks Asset Bundle that promotes one governed ETL through `dev -> qa -> staging -> prod`
with GitHub Actions, on Unity Catalog. Synthetic customer and card data carries PII and PCI
columns protected by row filters, column masks and tag-based (ABAC) policies.

```
.
├── databricks.yml                     # bundle, variables, run_as, permissions, 4 targets
├── resources/
│   ├── schemas.yml                    # bronze / silver / gold / governance + UC grants
│   ├── governed_etl.job.yml           # apply_governance -> ingest -> build_gold
│   └── gold_aggregates.pipeline.yml   # serverless Lakeflow Declarative Pipeline
├── src/
│   ├── governance/apply_governance.py # entitlements, row filter, column mask, tags, ABAC
│   ├── ingest/generate_synthetic_data.py
│   └── pipelines/gold/transformations/*.sql
├── infra/
│   ├── bootstrap_environments.py      # one-time platform setup: catalogs, SPs, bindings
│   └── configure_github.sh            # GitHub environments + SP OAuth secrets
└── .github/workflows/
    ├── ci.yml                         # PR: validate all targets, deploy + run dev
    ├── cd.yml                         # main: qa -> staging -> prod (prod needs approval)
    └── deploy.yml                     # reusable validate/deploy/run for one target
```

## 1. Environment topology

One Unity Catalog metastore. One catalog and one service principal per environment.

| Env | Catalog | Service principal | Isolation | Humans |
|---|---|---|---|---|
| dev | `dabs_demo_dev` | `dabs-demo-dev-sp` | ISOLATED, bound to the dev workspace | browse, read masked silver/gold |
| qa | `dabs_demo_qa` | `dabs-demo-qa-sp` | ISOLATED, bound to the qa workspace | browse, read masked silver/gold |
| staging | `dabs_demo_staging` | `dabs-demo-staging-sp` | ISOLATED, bound to the staging workspace | browse, read masked silver/gold |
| prod | `dabs_demo_prod` | `dabs-demo-prod-sp` | ISOLATED, bound to the prod workspace | read gold; approved readers see PII |

- Each catalog has its own managed storage root, so environments never share storage.
- Each catalog is owned by its environment SP. No human owns an environment.
- `ISOLATED` plus workspace bindings means a catalog is only visible in the workspaces it is
  bound to. This sandbox has one workspace, so all four bind to it. In a real deployment pass
  `--workspace-ids '{"dev":[111],"qa":[222],"staging":[333],"prod":[444]}'`. A prod catalog can
  also be bound read-only into a lower workspace for validation.
- The split is deliberate. `infra/` is platform-owned (normally Terraform) and runs once with
  admin rights. Everything inside a catalog is application-owned and deployed by the bundle.

```bash
uv run infra/bootstrap_environments.py --profile fe-sandbox-kevo \
  --storage-root s3://kevo-ext-s3-332745928618-hlfn1o/dabs_demo
```

Show: Catalog Explorer > `dabs_demo_prod` > Workspaces tab (binding), Permissions tab (SP owner).

## 2. CI/CD with Declarative Automation Bundles

`databricks.yml` defines four targets. They differ only in variables:

| | dev | qa | staging | prod |
|---|---|---|---|---|
| mode | development (name prefix, paused schedules) | production | production | production |
| catalog | `dabs_demo_dev` | `dabs_demo_qa` | `dabs_demo_staging` | `dabs_demo_prod` |
| run_as | dev SP | qa SP | staging SP | prod SP |
| customers | 5k | 20k | 50k | 100k |
| analyst regions | NA | NA | NA | ALL |
| PII readers | none | none | none | analyst |

Defined as code:

- **Jobs**: `resources/governed_etl.job.yml`, three tasks, job parameters bound to bundle variables.
- **Pipelines**: `resources/gold_aggregates.pipeline.yml`, serverless, with expectations in SQL.
- **Permissions**: top-level `permissions` (SP and `admins` CAN_MANAGE, `users` CAN_VIEW) on every
  job and pipeline; `grants` on schemas (`account users` SELECT on silver and gold, nothing on
  bronze); `run_as` the environment SP.
- **Identity resolution**: `env_sp` uses a `lookup` by display name, so no IDs are hard-coded.

Pipeline:

1. **PR** (`ci.yml`): `bundle validate` against all four targets, then `bundle deploy -t dev` and
   `bundle run governed_etl -t dev` as `dabs-demo-dev-sp`.
2. **Merge to main** (`cd.yml`): the same commit is validated, deployed and run in qa, then
   staging, then prod. Each stage uses its own GitHub environment and its own SP secret.
3. **Prod gate**: the `prod` GitHub environment requires a reviewer and only accepts `main`.

```bash
infra/configure_github.sh fe-sandbox-kevo KVN2099/dabs-demo-databricks
```

Live demo: open a PR that changes `customer_count` or adds a gold MV, watch `ci`, merge, approve prod.

Local inner loop: `databricks bundle deploy -t dev --profile fe-sandbox-kevo`. Development mode
prefixes every resource with your name (`[dev kevin_romero]`, `dev_kevin_romero_silver`), so
developers do not collide with each other or with CI.

## 3. Lower-environment data strategy

Lower environments get synthetic data with production shape. Controls are applied before data
lands (`apply_governance` runs first), and they are identical in every environment. Only the
entitlement variables change per target.

| Control | Where | Effect |
|---|---|---|
| Row filter `region_filter` | `silver.customers`, `silver.transactions` | caller sees only regions in `governance.entitlements` |
| Column mask `mask_email` | `silver.customers.email` | `a***@example.com` unless `pii_clear` |
| Governed tags | `class.us_ssn`, `class.phone_number`, `class.credit_card`, `class.email_address`, `class.name`, `class.date_of_birth` | classification travels with the column |
| ABAC `pii_mask_ssn`, `pii_mask_phone` | `silver` schema | any column tagged SSN/phone shows last 4 |
| ABAC `pci_mask_card_number` | `silver` schema | any column tagged credit card shows `**** **** **** 1234` |

ABAC policies apply `TO account users EXCEPT <env SP>, <pii_readers>`. In dev/qa/staging
`pii_readers` is empty, so every human sees masked data. In prod the analyst is listed, so the same
query returns clear values. A new table with a `class.credit_card` column is protected the moment
it is tagged, with no per-table work.

Demo queries (run as the analyst in each environment):

```sql
-- Lower env: NA rows only, email/SSN/phone/card masked
SELECT region, full_name, email, phone, ssn FROM dabs_demo_qa.silver.customers LIMIT 10;
SELECT card_number, amount FROM dabs_demo_qa.silver.transactions LIMIT 10;

-- Prod: all regions, clear values for the approved reader
SELECT region, full_name, email, phone, ssn FROM dabs_demo_prod.silver.customers LIMIT 10;

-- What is protected and why
SHOW POLICIES ON SCHEMA dabs_demo_qa.silver;
SELECT * FROM dabs_demo_qa.governance.entitlements;
SELECT column_name, tag_name FROM dabs_demo_qa.information_schema.column_tags WHERE schema_name = 'silver';

-- Gold has no PII and is unaffected
SELECT * FROM dabs_demo_qa.gold.daily_spend_by_region ORDER BY txn_date DESC LIMIT 10;
```

Notes:

- `class.*` are Databricks system governed tags, the same ones Data Classification assigns
  automatically. Custom governed tags (for example `sensitivity = pii | pci`) work the same way.
- Entitlements are a mapping table to keep the demo self-contained. In production, use account
  groups with `is_account_group_member()` and list groups in the ABAC `EXCEPT` clause.
- Bronze has no grants for humans. Only the environment SP (catalog owner) reads raw data.

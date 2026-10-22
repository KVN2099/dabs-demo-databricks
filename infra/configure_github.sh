#!/usr/bin/env bash
# Wire GitHub Actions to the per-environment service principals.
#
# For each env it creates a GitHub environment, stores DATABRICKS_HOST as a
# variable, mints an OAuth secret for dabs-demo-<env>-sp and stores the client
# ID/secret as environment secrets. `prod` requires a reviewer before deploys.
# Secrets are piped straight into `gh` and never printed.
#
# Usage: infra/configure_github.sh <databricks-profile> <owner/repo> [prod-reviewer]
set -euo pipefail

PROFILE="$1"
REPO="$2"
REVIEWER="${3:-${REPO%%/*}}"
HOST="$(databricks auth env --profile "$PROFILE" | jq -r '.env.DATABRICKS_HOST')"
REVIEWER_ID="$(gh api "users/$REVIEWER" --jq .id)"

for ENV in dev qa staging prod; do
  if [[ "$ENV" == "prod" ]]; then
    gh api -X PUT "repos/$REPO/environments/$ENV" --input - <<JSON >/dev/null
{"reviewers": [{"type": "User", "id": $REVIEWER_ID}], "deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true}}
JSON
    gh api -X POST "repos/$REPO/environments/$ENV/deployment-branch-policies" -f name=main -f type=branch >/dev/null 2>&1 || true
  else
    gh api -X PUT "repos/$REPO/environments/$ENV" >/dev/null
  fi

  SP_JSON="$(databricks service-principals list -p "$PROFILE" -o json --filter "displayName eq \"dabs-demo-$ENV-sp\"")"
  SP_ID="$(jq -r '.[0].id' <<<"$SP_JSON")"
  CLIENT_ID="$(jq -r '.[0].applicationId' <<<"$SP_JSON")"

  gh variable set DATABRICKS_HOST --env "$ENV" --repo "$REPO" --body "$HOST"
  gh secret set DATABRICKS_CLIENT_ID --env "$ENV" --repo "$REPO" --body "$CLIENT_ID"
  databricks service-principal-secrets-proxy create "$SP_ID" -p "$PROFILE" -o json \
    | jq -r .secret \
    | gh secret set DATABRICKS_CLIENT_SECRET --env "$ENV" --repo "$REPO"

  echo "configured $ENV -> dabs-demo-$ENV-sp ($CLIENT_ID)"
done

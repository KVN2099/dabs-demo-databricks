# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sdk>=0.60"]
# ///
"""Platform bootstrap: one catalog + one service principal per environment.

This is the part a platform team owns (usually Terraform). It runs once with
admin rights. Everything inside the catalogs (schemas, jobs, pipelines,
grants, masks, policies) is owned by the bundle and deployed by CI.

For each environment it:
  1. Creates a service principal `dabs-demo-<env>-sp` (the CI deploy + run identity).
  2. Creates catalog `dabs_demo_<env>` with its own managed storage root.
  3. Sets the catalog to ISOLATED and binds it to the workspace(s) for that env.
  4. Makes the env service principal the catalog owner (no human owns an env).
  5. Grants humans (`account users`) browse access on lower environments, plus
     CREATE SCHEMA in dev for personal development-mode deploys.

Usage:
  uv run infra/bootstrap_environments.py --profile fe-sandbox-kevo \
      --storage-root s3://my-bucket/dabs_demo
"""

import argparse
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest
from databricks.sdk.service import catalog, iam

ENVIRONMENTS = ["dev", "qa", "staging", "prod"]
LOWER_ENVIRONMENTS = {"dev", "qa", "staging"}


def ensure_service_principal(w: WorkspaceClient, env: str) -> iam.ServicePrincipal:
    name = f"dabs-demo-{env}-sp"
    existing = list(w.service_principals.list(filter=f'displayName eq "{name}"'))
    if existing:
        return existing[0]
    return w.service_principals.create(
        display_name=name,
        active=True,
        entitlements=[iam.ComplexValue(value="workspace-access")],
    )


def ensure_catalog(w: WorkspaceClient, env: str, storage_root: str) -> str:
    name = f"dabs_demo_{env}"
    # get() is denied for an ISOLATED catalog not yet bound here, so key off create.
    try:
        w.catalogs.create(
            name=name,
            storage_root=f"{storage_root.rstrip('/')}/{env}",
            comment=f"DABs demo: {env} environment catalog",
            properties={"environment": env},
        )
    except BadRequest as e:
        if "already exists" not in str(e):
            raise
    return name


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--storage-root", required=True, help="Base path for per-env managed storage")
    parser.add_argument(
        "--workspace-ids",
        default="",
        help="JSON map env -> [workspace ids]. Defaults to the current workspace for every env.",
    )
    args = parser.parse_args()

    w = WorkspaceClient(profile=args.profile)
    current_ws = w.metastores.current().workspace_id
    bindings = json.loads(args.workspace_ids) if args.workspace_ids else {}

    summary = {}
    for env in ENVIRONMENTS:
        sp = ensure_service_principal(w, env)
        cat_name = ensure_catalog(w, env, args.storage_root)

        w.workspace_bindings.update_bindings(
            securable_type="catalog",
            securable_name=cat_name,
            add=[
                catalog.WorkspaceBinding(
                    workspace_id=ws_id,
                    binding_type=catalog.WorkspaceBindingBindingType.BINDING_TYPE_READ_WRITE,
                )
                for ws_id in bindings.get(env, [current_ws])
            ],
        )
        w.catalogs.update(cat_name, isolation_mode=catalog.CatalogIsolationMode.ISOLATED)

        changes = [
            catalog.PermissionsChange(
                principal=sp.application_id,
                add=[catalog.Privilege.ALL_PRIVILEGES, catalog.Privilege.MANAGE],
            )
        ]
        if env in LOWER_ENVIRONMENTS:
            human_privileges = [catalog.Privilege.USE_CATALOG, catalog.Privilege.BROWSE]
            if env == "dev":
                # Developers deploy their own prefixed copy of the bundle into dev.
                human_privileges.append(catalog.Privilege.CREATE_SCHEMA)
            changes.append(
                catalog.PermissionsChange(principal="account users", add=human_privileges)
            )
        w.grants.update(securable_type="catalog", full_name=cat_name, changes=changes)
        w.catalogs.update(cat_name, owner=sp.application_id)

        summary[env] = {
            "catalog": cat_name,
            "service_principal": sp.display_name,
            "application_id": sp.application_id,
            "scim_id": sp.id,
        }

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

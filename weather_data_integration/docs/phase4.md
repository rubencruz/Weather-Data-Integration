# Phase 4 - CI/CD, DEV and PROD

Phase 4 turns the project into a deployable Databricks application.

## Environments

```text
GitHub
  |
  +-- develop --> Databricks DEV --> weather_dev
  |
  +-- main -----> Databricks PROD --> weather_prod
```

## CI

Every pull request and push to `develop` or `main` runs:

1. Python dependency installation
2. PyTest unit tests
3. Databricks bundle validation

## Deployment

- `develop` deploys to the DEV bundle target.
- `main` deploys to the PROD bundle target.
- GitHub Environments named `development` and `production` are used so production can require manual approval.

## Required GitHub secrets

```text
DATABRICKS_HOST
DATABRICKS_TOKEN
```

For production, use a dedicated Databricks service principal/token when the workspace edition and account configuration support it. For a personal learning workspace, a user token can be used while keeping the production GitHub Environment protected.

## Delta Lake target

The pipeline uses Unity Catalog managed Delta tables. No PostgreSQL or external JDBC connectivity is required.

For Bundle validation/deployment, the workflows expose the workspace URL both as `DATABRICKS_HOST` for CLI authentication and as `DATABRICKS_BUNDLE_VAR_databricks_host` for the bundle variable.

Local example:

```bash
export DATABRICKS_HOST="https://<workspace-host>"
export DATABRICKS_TOKEN="<token>"
export DATABRICKS_BUNDLE_VAR_databricks_host="$DATABRICKS_HOST"

databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run weather_pipeline -t dev
```

# UC Data Quality Explorer

A Databricks App that scans Unity Catalog metadata to find duplicate datasets across schemas, recommends gold-standard tables, and surfaces group-level access permissions — helping data architects and engineers clean up data sprawl before it reaches analysts.

## What it does

| Feature | Description |
|---|---|
| **Catalog Scanner** | Reads every schema and table in a catalog, including column types, row counts, comments, and update timestamps. |
| **Permissions Viewer** | Fetches schema- and catalog-level grants (via the UC Permissions API) so you can see which groups have READ / WRITE access to each asset. |
| **Duplicate Detection** | Uses Jaccard column-name similarity, type compatibility scoring, and fuzzy table-name matching to cluster tables that represent the same entity across schemas. |
| **Gold Standard Scoring** | Ranks each table in a duplicate group on completeness, documentation, naming convention, schema tier, freshness, and row count to recommend the "gold" dataset analysts should use. |
| **Table Comparison** | Side-by-side column diff, permissions diff, and sample data for any two tables. |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Databricks App                      │
│                                                      │
│  ┌──────────────┐       ┌──────────────────────────┐ │
│  │   Frontend    │       │     FastAPI Backend       │ │
│  │  (Vanilla JS) │◄────►│                          │ │
│  │              │       │  /api/catalog/*           │ │
│  │  Dashboard   │       │  /api/duplicates/*        │ │
│  │  Catalog     │       │  /api/compare/*           │ │
│  │  Duplicates  │       │                          │ │
│  │  Compare     │       │  scanner.py  (UC SDK)    │ │
│  └──────────────┘       │  duplicates.py           │ │
│                         │  comparator.py           │ │
│                         └────────┬─────────────────┘ │
└──────────────────────────────────┼───────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │      Databricks APIs         │
                    │                              │
                    │  Unity Catalog  (metadata)   │
                    │  SQL Statement  (queries)    │
                    │  UC Permissions (grants)     │
                    └──────────────────────────────┘
```

## Project structure

```
dfe-data-quality-app/
├── databricks.yml          # DAB bundle config (targets, variables)
├── app.yaml                # App runtime config (command, env vars)
├── app.py                  # FastAPI entrypoint
├── requirements.txt        # Python dependencies
├── server/
│   ├── config.py           # Dual-mode auth (local CLI / deployed App)
│   ├── scanner.py          # UC metadata scanner + permissions fetcher
│   ├── duplicates.py       # Duplicate detection + gold standard scoring
│   ├── comparator.py       # Table comparison + sample data
│   └── routes/
│       ├── catalog.py      # /api/catalog/*
│       ├── duplicates.py   # /api/duplicates/*
│       └── compare.py      # /api/compare/*
├── frontend/
│   └── dist/
│       ├── index.html
│       └── assets/
│           ├── style.css
│           ├── api.js
│           └── app.js
└── scripts/
    ├── deploy.sh           # One-command deploy (bundle + app source)
    └── generate_data.py    # Test data generator (20 tables, 5 schemas)
```

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) >= 0.230
- Python 3.10+
- A Databricks workspace with Unity Catalog enabled
- A SQL warehouse (Serverless or Pro)
- Permissions to create schemas, tables, and apps in the target catalog

## Quick start

### 1. Authenticate

```bash
databricks auth login --host https://<WORKSPACE>.cloud.databricks.com
```

Or configure a named profile:

```bash
databricks auth login \
  --host https://<WORKSPACE>.cloud.databricks.com \
  --profile my-workspace
```

### 2. Configure for your workspace

Two files need updating:

**`app.yaml`** — set `CATALOG_NAME` and `WAREHOUSE_ID` in the `env` section:

```yaml
env:
  - name: CATALOG_NAME
    value: "my_catalog"
  - name: WAREHOUSE_ID
    value: "abc123def456"
```

**`databricks.yml`** — set matching values in the target:

```yaml
targets:
  dev:
    workspace:
      host: https://<WORKSPACE>.cloud.databricks.com
    variables:
      catalog_name: my_catalog
      warehouse_id: abc123def456
```

You can find your SQL warehouse ID in the Databricks UI under **SQL Warehouses**, or with:

```bash
databricks warehouses list
```

### 3. (Optional) Generate test data

If you want to create the demo education datasets (20 tables across 5 schemas with deliberate duplicates):

```bash
python scripts/generate_data.py \
  --catalog my_catalog \
  --warehouse abc123def456 \
  --profile my-workspace
```

This creates:

| Schema | Tables | Purpose |
|---|---|---|
| `bronze` | `raw_students`, `raw_schools`, `raw_exam_results`, `raw_attendance` | Raw ingestion layer with original column names |
| `silver` | `students`, `schools`, `exam_results`, `attendance` | Cleaned and standardised |
| `gold` | `dim_students`, `dim_schools`, `fact_exam_results`, `fact_attendance_agg` | Curated with SCD tracking, aggregations, and documentation |
| `team_analytics` | `student_data`, `school_info`, `exam_scores`, `student_attendance` | Duplicate set with renamed columns |
| `team_reporting` | `pupils`, `school_directory`, `assessment_results`, `attendance_register` | Another duplicate set with different naming |

The script also applies table comments and grants to workspace groups (`data_engineers`, `data_analysts`, `reporting_team`).

### 4. Deploy

The easiest path is the one-command deploy script:

```bash
./scripts/deploy.sh
```

Or with a named profile:

```bash
./scripts/deploy.sh --profile my-workspace
```

This runs `databricks bundle deploy` (syncs files + creates the app resource) followed by `databricks apps deploy` (deploys the source code and starts the app).

**Manual deploy** (if you prefer step by step):

```bash
# 1. Validate
databricks bundle validate

# 2. Deploy bundle (syncs files to workspace, creates/updates app resource)
databricks bundle deploy

# 3. Start the app if stopped
databricks apps start dfe-data-quality

# 4. Deploy source code from the bundle's workspace path
databricks apps deploy dfe-data-quality \
  --source-code-path /Workspace/Users/<you>/.bundle/dfe-data-quality/dev/files
```

### 5. Grant permissions to the app service principal

The first deployment creates a new service principal for the app. Find its ID in the deploy output or with:

```bash
databricks apps get dfe-data-quality | grep service_principal_client_id
```

Then grant it access to the catalog and warehouse — see the [App permissions](#app-permissions) section below.

### 6. Open the app

```bash
databricks apps get dfe-data-quality
```

The `url` field contains the app URL. The app requires workspace authentication — log into the workspace in your browser first, then open the app URL.

## App permissions

The Databricks App runs under a **service principal** that is automatically created during deployment. This SP needs:

| Permission | Scope | Reason |
|---|---|---|
| `USE CATALOG` | Target catalog | Browse schemas |
| `USE SCHEMA` | All schemas in the catalog | List tables |
| `SELECT` | All schemas in the catalog | Read table metadata and sample data |
| `MANAGE` | Target catalog | Read permission grants via the UC Permissions API |
| `CAN_USE` | SQL warehouse | Execute row-count and sample-data queries |

Grant these after the first deployment:

```sql
-- Replace with your catalog, SP ID is in the deployment logs
GRANT USE CATALOG ON CATALOG my_catalog TO `<service-principal-uuid>`;
GRANT USE SCHEMA ON SCHEMA my_catalog.* TO `<service-principal-uuid>`;
GRANT SELECT ON SCHEMA my_catalog.* TO `<service-principal-uuid>`;
GRANT MANAGE ON CATALOG my_catalog TO `<service-principal-uuid>`;
```

For the warehouse, use the Permissions API or the UI to grant `CAN_USE` to the service principal.

## API reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/catalog/scan` | Trigger a full catalog scan (metadata + permissions + row counts) |
| `GET` | `/api/catalog/schemas` | List scanned schemas |
| `GET` | `/api/catalog/tables?schema=gold` | List tables, optionally filtered by schema |
| `GET` | `/api/catalog/table/{schema}/{table}` | Get full metadata for one table |
| `GET` | `/api/duplicates/detect?threshold=0.5` | Detect duplicate groups above the similarity threshold |
| `GET` | `/api/duplicates/groups` | Return cached duplicate groups |
| `GET` | `/api/compare/{s1}/{t1}/{s2}/{t2}` | Column + permissions diff between two tables |
| `GET` | `/api/compare/sample/{schema}/{table}` | Fetch 10 sample rows from a table |

## Customisation

### Similarity weights

The duplicate detection algorithm uses three signals combined with configurable weights (in `server/duplicates.py`):

- **Column similarity** (50%) — Jaccard index on canonical column names (with synonym mapping)
- **Type similarity** (30%) — Proportion of shared columns with compatible types
- **Name similarity** (20%) — Token-based Jaccard on table names, stripping `raw_`, `dim_`, `fact_` prefixes

### Gold standard scoring

Each table in a duplicate group is scored (in `server/duplicates.py`) on:

- Column completeness (25 pts)
- Documentation / comments (20 pts)
- Naming convention — `dim_` / `fact_` prefix (15 pts)
- Schema tier — `gold` schema (20 pts)
- Freshness — most recently updated (10 pts)
- Row count — largest dataset (10 pts)

### Adding synonym mappings

Edit the `_SYNONYMS` dictionary in `server/duplicates.py` to add domain-specific column name mappings. This improves cross-team duplicate detection when teams use different naming conventions for the same fields.

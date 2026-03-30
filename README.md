# UC Data Duplicates

A Databricks App that scans Unity Catalog metadata to find duplicate datasets across schemas, recommends gold-standard tables, and surfaces group-level access permissions — helping data architects and engineers clean up data sprawl before it reaches analysts.

## Features

| Feature | Description |
|---|---|
| **Catalog Scanner** | Reads every schema and table in a catalog — columns, types, row counts, comments, timestamps. |
| **Permissions Viewer** | Shows which groups and users have READ / WRITE access to each table (via UC Permissions API). |
| **Duplicate Detection** | Clusters tables that represent the same entity using column-name Jaccard similarity, type compatibility, and fuzzy table-name matching. |
| **Gold Standard Scoring** | Ranks each duplicate on completeness, documentation, naming convention, schema tier, freshness, and row count to recommend the canonical dataset. |
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
uc-data-duplicates/
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
│   └── dist/               # Static SPA (HTML/CSS/JS, no build step)
└── scripts/
    ├── deploy.sh           # One-command deploy (bundle + app source)
    └── generate_data.py    # Optional test data generator
```

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) >= 0.230
- Python 3.10+
- A Databricks workspace with:
  - Unity Catalog enabled
  - A SQL warehouse (Serverless or Pro)
  - Permission to create Apps

## Getting started

### 1. Clone and authenticate

```bash
git clone <repo-url> && cd uc-data-duplicates

databricks auth login --host https://<WORKSPACE>.cloud.databricks.com
```

### 2. Find your warehouse ID

```bash
databricks warehouses list --output json | python3 -c "
import json, sys
for w in json.load(sys.stdin):
    print(f'{w[\"id\"]}  {w[\"name\"]}  ({w[\"state\"]})')
"
```

### 3. Configure

Edit **two files** with your workspace details:

**`app.yaml`** — the app reads these at runtime:

```yaml
env:
  - name: CATALOG_NAME
    value: "my_catalog"
  - name: WAREHOUSE_ID
    value: "abc123def456"
```

**`databricks.yml`** — the bundle uses these for deployment:

```yaml
targets:
  dev:
    workspace:
      host: https://<WORKSPACE>.cloud.databricks.com
    variables:
      catalog_name: my_catalog
      warehouse_id: abc123def456
```

### 4. (Optional) Generate test data

Creates 20 education-themed tables across 5 schemas (bronze, silver, gold, team_analytics, team_reporting) with deliberate duplicates:

```bash
python3 scripts/generate_data.py \
  --catalog my_catalog \
  --warehouse abc123def456
```

| Schema | Tables | Purpose |
|---|---|---|
| `bronze` | `raw_students`, `raw_schools`, `raw_exam_results`, `raw_attendance` | Raw ingestion layer with original column names |
| `silver` | `students`, `schools`, `exam_results`, `attendance` | Cleaned and standardised |
| `gold` | `dim_students`, `dim_schools`, `fact_exam_results`, `fact_attendance_agg` | Curated with SCD tracking, aggregations, and documentation |
| `team_analytics` | `student_data`, `school_info`, `exam_scores`, `student_attendance` | Duplicate set with renamed columns |
| `team_reporting` | `pupils`, `school_directory`, `assessment_results`, `attendance_register` | Another duplicate set with different naming |

### 5. Deploy

```bash
./scripts/deploy.sh
```

Or step by step:

```bash
databricks bundle validate
databricks bundle deploy
databricks apps start uc-data-duplicates
databricks apps deploy uc-data-duplicates \
  --source-code-path /Workspace/Users/<you>/.bundle/uc-data-duplicates/dev/files
```

### 6. Grant permissions to the app service principal

The deployment creates a service principal. Find its ID:

```bash
databricks apps get uc-data-duplicates --output json | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(d['service_principal_client_id'])
"
```

Then grant it access (replace `<SP_ID>` and `<CATALOG>`):

```sql
GRANT USE CATALOG ON CATALOG <CATALOG> TO `<SP_ID>`;
GRANT USE SCHEMA ON SCHEMA <CATALOG>.* TO `<SP_ID>`;
GRANT SELECT ON SCHEMA <CATALOG>.* TO `<SP_ID>`;
GRANT MANAGE ON CATALOG <CATALOG> TO `<SP_ID>`;
```

And grant `CAN_USE` on the SQL warehouse via the UI or:

```bash
databricks api patch /api/2.0/permissions/sql/warehouses/<WAREHOUSE_ID> \
  --json '{"access_control_list":[{"service_principal_name":"<SP_ID>","permission_level":"CAN_USE"}]}'
```

### 7. Open the app

```bash
databricks apps get uc-data-duplicates
```

Open the `url` from the output in your browser. You must be logged into the workspace first.

## Using the app

1. **Dashboard** — click **Scan Catalog** to fetch all metadata. Stat cards show schema, table, and column counts plus detected duplicate groups.
2. **Catalog Explorer** — browse the schema tree, click a table to see columns, row count, owner, comments, and group permissions.
3. **Duplicates** — view duplicate clusters with similarity scores and gold-standard badges. Adjust the threshold slider and re-detect.
4. **Compare** — pick any two tables for a side-by-side column diff, permissions comparison, and sample data preview.

## API reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/catalog/scan` | Full catalog scan (metadata + permissions + row counts) |
| `GET` | `/api/catalog/schemas` | List scanned schemas |
| `GET` | `/api/catalog/tables?schema=gold` | List tables, optionally filtered by schema |
| `GET` | `/api/catalog/table/{schema}/{table}` | Full metadata for one table |
| `GET` | `/api/duplicates/detect?threshold=0.5` | Detect duplicate groups above the similarity threshold |
| `GET` | `/api/duplicates/groups` | Return cached duplicate groups |
| `GET` | `/api/compare/{s1}/{t1}/{s2}/{t2}` | Column + permissions diff between two tables |
| `GET` | `/api/compare/sample/{schema}/{table}` | Fetch 10 sample rows from a table |

## Customisation

### Similarity weights

Edit `server/duplicates.py` — the `detect_duplicates` function accepts:

- `col_weight` (default 0.50) — Jaccard index on canonical column names
- `type_weight` (default 0.30) — proportion of shared columns with compatible types
- `name_weight` (default 0.20) — token-based Jaccard on table names

### Gold standard scoring

Each table in a duplicate group is scored on:

- Column completeness (25 pts)
- Documentation / comments (20 pts)
- Naming convention — `dim_` / `fact_` prefix (15 pts)
- Schema tier — `gold` schema (20 pts)
- Freshness (10 pts)
- Row count (10 pts)

### Synonym mappings

Edit the `_SYNONYMS` dictionary in `server/duplicates.py` to add domain-specific column name mappings (e.g. `student_id` = `learner_id` = `pupil_id`). This improves duplicate detection when teams use different naming conventions.

## License

MIT

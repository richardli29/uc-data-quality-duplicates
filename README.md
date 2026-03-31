# UC Data Quality Duplicates

A Databricks App that scans Unity Catalog metadata across all accessible catalogs to find duplicate datasets, recommends gold-standard tables, and surfaces group-level access permissions — helping data architects and engineers clean up data sprawl before it reaches analysts.

## Features

| Feature | Description |
|---|---|
| **Multi-Catalog Scanner** | Scans every accessible catalog in one click — schemas, tables, columns, types, row counts, comments, timestamps. Shows which catalogs were scanned with per-catalog breakdowns. |
| **Permissions Viewer** | Shows which groups and users have READ / WRITE access to each table via `system.information_schema` — no `MANAGE` privilege needed. |
| **Duplicate Detection** | Clusters tables that represent the same entity using column-name Jaccard similarity, type compatibility, and fuzzy table-name matching. Groups are labelled by the common entity name (e.g. "Students", "Exam Results") rather than generic IDs. |
| **Gold Standard Scoring** | Ranks each duplicate on completeness, documentation, naming convention, schema tier, freshness, and row count to recommend the canonical dataset. |
| **Table Comparison** | Side-by-side column diff, permissions diff, and sample data for any two tables. |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Databricks App                        │
│                                                          │
│  ┌──────────────┐       ┌──────────────────────────────┐ │
│  │   Frontend    │       │     FastAPI Backend           │ │
│  │  (Vanilla JS) │◄────►│                              │ │
│  │              │       │  /api/catalog/list            │ │
│  │  Dashboard   │       │  /api/catalog/scan-all         │ │
│  │  Catalog     │       │  /api/duplicates/*            │ │
│  │  Duplicates  │       │  /api/compare/*               │ │
│  │  Compare     │       │                              │ │
│  └──────────────┘       │  scanner.py  (UC SDK + SQL)  │ │
│                         │  duplicates.py               │ │
│                         │  comparator.py               │ │
│                         └────────┬─────────────────────┘ │
└──────────────────────────────────┼───────────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │        Databricks APIs           │
                    │                                  │
                    │  Unity Catalog SDK  (metadata)   │
                    │  SQL Statement API  (queries)    │
                    │  system.information_schema       │
                    │    (permissions — no MANAGE)     │
                    └──────────────────────────────────┘
```

## Project structure

```
uc-data-quality-duplicates/
├── databricks.yml          # DAB bundle config (targets, variables)
├── app.yaml                # App runtime config (command, env vars)
├── app.py                  # FastAPI entrypoint
├── requirements.txt        # Python dependencies
├── server/
│   ├── config.py           # Dual-mode auth (local CLI / deployed App)
│   ├── scanner.py          # UC metadata scanner + permissions (via SQL)
│   ├── duplicates.py       # Duplicate detection + gold standard scoring
│   ├── comparator.py       # Table comparison + sample data
│   └── routes/
│       ├── catalog.py      # /api/catalog/*  (list, scan-all, schemas, tables)
│       ├── duplicates.py   # /api/duplicates/*
│       └── compare.py      # /api/compare/*
├── frontend/
│   └── dist/               # Static SPA (HTML/CSS/JS, no build step)
└── scripts/
    ├── deploy.sh                  # One-command deploy (bundle + app source)
    ├── create_governance_views.sql # MVs for permissions (run once per catalog)
    ├── generate_data.py           # Test data generator (Python + CLI)
    └── generate_data.sql          # Test data generator (pure SQL)
```

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) >= 0.230
- Python 3.10+
- A Databricks workspace with:
  - Unity Catalog enabled
  - A SQL warehouse (Serverless or Pro)
  - System tables enabled (for permissions via `system.information_schema`)
  - Permission to create Apps

## Permissions

The app's service principal needs **no `MANAGE` privilege**. Permissions are read via `system.information_schema` SQL views instead of the UC Permissions REST API.

### Required grants for the service principal

| Privilege | Scope | Purpose |
|---|---|---|
| `USE CATALOG` | Each catalog to scan | List schemas and tables |
| `USE SCHEMA` | Each schema to scan | List tables within schemas |
| `SELECT` | Each schema to scan | Row counts and sample data queries |
| `USE SCHEMA` | `<catalog>.governance` | Access governance materialized views |
| `SELECT` | `<catalog>.governance` | Read permissions (catalog/schema/table grants) |
| `CAN_USE` | SQL warehouse | Execute SQL queries |

### Quick setup

**Step 1 — Create governance materialized views** (run once per catalog):

Open `scripts/create_governance_views.sql` in a Databricks SQL editor, set the widget to your catalog name, and run all statements. This creates MVs in `<catalog>.governance` that mirror `system.information_schema` privilege tables using **definer rights** — so the SP can read permissions without needing access to the `system` catalog.

> Any user with `SELECT` on `system.information_schema` can create these MVs (typically all workspace users).

**Step 2 — Grant the SP access** (per catalog):

```sql
-- Catalog + schema access
GRANT USE CATALOG ON CATALOG <CATALOG> TO `<SP_ID>`;
GRANT USE SCHEMA ON SCHEMA <CATALOG>.<schema> TO `<SP_ID>`;  -- repeat per schema
GRANT SELECT ON SCHEMA <CATALOG>.<schema> TO `<SP_ID>`;      -- repeat per schema

-- Governance MVs (permissions viewer)
GRANT USE SCHEMA ON SCHEMA <CATALOG>.governance TO `<SP_ID>`;
GRANT SELECT ON SCHEMA <CATALOG>.governance TO `<SP_ID>`;
```

### How permissions reading works

The app tries three sources in order:
1. `<catalog>.governance.*_privileges` — materialized views (recommended, no MANAGE needed)
2. `system.information_schema.*_privileges` — direct system tables (requires metastore admin to grant)
3. Degrades gracefully — shows "No grants found" if neither source is accessible

### Alternative: BROWSE privilege

For read-only metadata access across all current and future catalogs:

```sql
GRANT BROWSE ON METASTORE TO `<SP_ID>`;
```

`BROWSE` covers metadata listing. You still need per-catalog `SELECT` for row counts and sample data.

## Getting started

### 1. Clone and authenticate

```bash
git clone https://github.com/richardli29/uc-data-quality-duplicates.git && cd uc-data-quality-duplicates

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

**`app.yaml`** — set the SQL warehouse for runtime queries:

```yaml
env:
  - name: WAREHOUSE_ID
    value: "abc123def456"
```

**`databricks.yml`** — set the workspace and warehouse for deployment:

```yaml
targets:
  dev:
    workspace:
      host: https://<WORKSPACE>.cloud.databricks.com
    variables:
      warehouse_id: abc123def456
```

> **Note:** `CATALOG_NAME` is not required. The app discovers and scans all accessible catalogs automatically.

### 4. (Optional) Generate test data

Creates 20 education-themed tables across 5 schemas with deliberate duplicates.

**Option A — SQL notebook** (paste into a Databricks SQL editor):

```bash
# Open scripts/generate_data.sql and set the widget to your catalog name
```

**Option B — Python CLI**:

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

Find the SP ID:

```bash
databricks apps get uc-data-duplicates --output json | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(d['service_principal_client_id'])
"
```

Then grant access (replace `<SP_ID>` and `<CATALOG>`):

```sql
-- Access to catalogs you want to scan (repeat per schema)
GRANT USE CATALOG ON CATALOG <CATALOG> TO `<SP_ID>`;
GRANT USE SCHEMA ON SCHEMA <CATALOG>.<schema> TO `<SP_ID>`;
GRANT SELECT ON SCHEMA <CATALOG>.<schema> TO `<SP_ID>`;

-- Access to governance MVs for permissions (no MANAGE needed)
GRANT USE SCHEMA ON SCHEMA <CATALOG>.governance TO `<SP_ID>`;
GRANT SELECT ON SCHEMA <CATALOG>.governance TO `<SP_ID>`;
```

And grant `CAN_USE` on the SQL warehouse:

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

1. **Dashboard** — click **Scan All Catalogs** to scan every accessible catalog at once. Stat cards show total schemas, tables, and columns plus per-catalog breakdowns and detected duplicate groups.
2. **Catalog Explorer** — browse all catalogs in a tree (catalog > schema > table), click a table to see columns, row count, owner, comments, and group permissions.
3. **Duplicates** — view duplicate clusters labelled by entity name (e.g. "Students", "Exam Results") with similarity scores and gold-standard badges. Adjust the threshold slider and re-detect. Cross-catalog duplicates are detected too.
4. **Compare** — pick any two tables (from any catalog) for a side-by-side column diff, permissions comparison, and sample data preview.

## API reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/catalog/list` | List all accessible catalogs |
| `GET` | `/api/catalog/scan-all` | Scan all catalogs (metadata + permissions + row counts) |
| `GET` | `/api/catalog/schemas?catalog=X` | List scanned schemas (optional catalog filter) |
| `GET` | `/api/catalog/tables?schema=gold&catalog=X` | List tables, optionally filtered by schema/catalog |
| `GET` | `/api/catalog/table/{catalog}/{schema}/{table}` | Full metadata for one table |
| `GET` | `/api/duplicates/detect?threshold=0.5` | Detect duplicate groups above the similarity threshold |
| `GET` | `/api/duplicates/groups` | Return cached duplicate groups |
| `GET` | `/api/compare/{cat1}/{s1}/{t1}/{cat2}/{s2}/{t2}` | Column + permissions diff between two tables |
| `GET` | `/api/compare/sample/{catalog}/{schema}/{table}` | Fetch 10 sample rows from a table |

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

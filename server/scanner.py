"""Unity Catalog metadata scanner. Collects table/column info and permissions.

All metadata is read from snapshot tables in
``catalog_40_copper_uc_metadata.metadata`` — mirrors of
``system.information_schema`` that provide definer-rights access.

These snapshot tables are refreshed weekly by a scheduled CTAS job.
Row counts are still fetched live from the actual tables.
"""

from __future__ import annotations
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from typing import Optional

from server.config import get_workspace_client, WAREHOUSE_ID

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    name: str
    type_name: str
    position: int
    comment: Optional[str] = None
    nullable: bool = True


@dataclass
class PermissionGrant:
    principal: str
    privileges: list[str] = field(default_factory=list)
    inherited_from: Optional[str] = None


@dataclass
class TableInfo:
    catalog: str
    schema: str
    name: str
    full_name: str
    table_type: str
    columns: list[ColumnInfo] = field(default_factory=list)
    row_count: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    owner: Optional[str] = None
    comment: Optional[str] = None
    permissions: list[PermissionGrant] = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


@dataclass
class SchemaInfo:
    catalog: str
    name: str
    full_name: str
    table_count: int = 0
    owner: Optional[str] = None
    comment: Optional[str] = None


class CatalogScanner:

    _METADATA_SOURCE = "catalog_40_copper_uc_metadata.metadata"

    # Catalogs to exclude from scan results
    _SKIP_CATALOG_NAMES = {"system", "samples", "__databricks_internal"}

    def __init__(self):
        self._client = None
        self._tables: list[TableInfo] = []
        self._schemas: list[SchemaInfo] = []
        self._scanned = False
        self._scanned_catalogs: list[str] = []

    @property
    def client(self):
        if self._client is None:
            self._client = get_workspace_client()
        return self._client

    def reset_client(self):
        self._client = None

    @property
    def is_scanned(self):
        return self._scanned

    @property
    def scanned_catalogs(self) -> list[str]:
        return list(self._scanned_catalogs)

    # ── SQL execution ─────────────────────────────────────────────────────

    def _run_sql(self, sql: str) -> list[list] | None:
        """Execute a SQL statement via the SQL Statement API and return rows."""
        try:
            raw = self.client.api_client.do(
                "POST",
                "/api/2.0/sql/statements/",
                body={
                    "statement": sql,
                    "warehouse_id": WAREHOUSE_ID,
                    "wait_timeout": "30s",
                },
            )
            data = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
            if data.get("status", {}).get("state") == "SUCCEEDED":
                return data.get("result", {}).get("data_array", [])
        except Exception as e:
            logger.debug(f"SQL query failed: {e}")
        return None

    # ── Catalog listing ───────────────────────────────────────────────────

    def list_catalogs(self) -> list[dict]:
        """Return all catalogs from the metadata snapshot, filtering system catalogs."""
        rows = self._run_sql(
            f"SELECT catalog_name, catalog_owner, comment "
            f"FROM {self._METADATA_SOURCE}.catalogs"
        )
        if not rows:
            return []

        result = []
        for row in rows:
            name = row[0]
            if name in self._SKIP_CATALOG_NAMES:
                continue
            result.append({
                "name": name,
                "owner": row[1],
                "comment": row[2],
            })
        return result

    # ── Full scan ─────────────────────────────────────────────────────────

    def scan_all(self) -> dict:
        """Scan every accessible catalog and accumulate results."""
        self.reset_client()
        self._tables = []
        self._schemas = []
        self._scanned_catalogs = []

        catalogs = self.list_catalogs()
        per_catalog = {}

        for cat_info in catalogs:
            name = cat_info["name"]
            try:
                stats = self._scan_one(name)
                per_catalog[name] = stats
                self._scanned_catalogs.append(name)
            except Exception as e:
                logger.warning(f"Failed to scan catalog {name}: {e}")
                per_catalog[name] = {"error": str(e), "schema_count": 0,
                                     "table_count": 0, "column_count": 0}

        self._scanned = True

        return {
            "catalogs_scanned": list(self._scanned_catalogs),
            "per_catalog": per_catalog,
            "total": {
                "catalog_count": len(self._scanned_catalogs),
                "schema_count": len(self._schemas),
                "table_count": len(self._tables),
                "column_count": sum(len(t.columns) for t in self._tables),
            },
        }

    def _scan_one(self, catalog: str) -> dict:
        """Scan a single catalog using the metadata snapshot tables."""
        logger.info(f"Scanning catalog: {catalog}")
        src = self._METADATA_SOURCE

        # ── Schemas ───────────────────────────────────────────────────────
        schema_rows = self._run_sql(
            f"SELECT schema_name, schema_owner, comment "
            f"FROM {src}.schemata "
            f"WHERE catalog_name = '{catalog}' "
            f"  AND schema_name != 'information_schema'"
        )

        local_schemas: list[SchemaInfo] = []
        if schema_rows:
            for row in schema_rows:
                local_schemas.append(SchemaInfo(
                    catalog=catalog,
                    name=row[0],
                    full_name=f"{catalog}.{row[0]}",
                    owner=row[1],
                    comment=row[2],
                ))

        # ── Tables ────────────────────────────────────────────────────────
        table_rows = self._run_sql(
            f"SELECT table_name, table_schema, table_type, "
            f"       table_owner, comment, created, last_altered "
            f"FROM {src}.tables "
            f"WHERE table_catalog = '{catalog}' "
            f"  AND table_schema != 'information_schema'"
        )

        local_tables: list[TableInfo] = []
        if table_rows:
            for row in table_rows:
                local_tables.append(TableInfo(
                    catalog=catalog,
                    schema=row[1],
                    name=row[0],
                    full_name=f"{catalog}.{row[1]}.{row[0]}",
                    table_type=row[2] or "UNKNOWN",
                    row_count=None,
                    created_at=row[5],
                    updated_at=row[6],
                    owner=row[3],
                    comment=row[4],
                ))

        # ── Columns ───────────────────────────────────────────────────────
        col_rows = self._run_sql(
            f"SELECT table_schema, table_name, column_name, "
            f"       full_data_type, ordinal_position, is_nullable, comment "
            f"FROM {src}.columns "
            f"WHERE table_catalog = '{catalog}' "
            f"  AND table_schema != 'information_schema' "
            f"ORDER BY table_schema, table_name, ordinal_position"
        )

        if col_rows:
            col_lookup: dict[tuple, list[ColumnInfo]] = defaultdict(list)
            for row in col_rows:
                col_lookup[(row[0], row[1])].append(ColumnInfo(
                    name=row[2],
                    type_name=row[3] or "unknown",
                    position=int(row[4]) if row[4] else 0,
                    nullable=row[5] != "NO",
                    comment=row[6],
                ))
            for table in local_tables:
                table.columns = col_lookup.get((table.schema, table.name), [])

        # ── Schema table counts ───────────────────────────────────────────
        schema_table_counts: dict[str, int] = defaultdict(int)
        for t in local_tables:
            schema_table_counts[t.schema] += 1
        for s in local_schemas:
            s.table_count = schema_table_counts.get(s.name, 0)

        # ── Permissions & row counts ──────────────────────────────────────
        self._fetch_permissions(catalog, local_tables)
        self._fetch_row_counts(local_tables)

        self._tables.extend(local_tables)
        self._schemas.extend(local_schemas)

        return {
            "catalog": catalog,
            "schema_count": len(local_schemas),
            "table_count": len(local_tables),
            "column_count": sum(len(t.columns) for t in local_tables),
        }

    # ── Permissions fetching ──────────────────────────────────────────────

    def _fetch_permissions(self, catalog: str, tables: list[TableInfo]):
        """Fetch permissions from the metadata snapshot tables."""
        source = self._METADATA_SOURCE
        logger.info(f"Permissions source for {catalog}: {source}")
        self._fetch_permissions_from(source, catalog, tables)

    def _fetch_permissions_from(
        self, source: str, catalog: str, tables: list[TableInfo]
    ):
        """Fetch catalog/schema/table privileges from the given source."""

        # ── Catalog-level grants ──────────────────────────────────────────
        catalog_grants: list[PermissionGrant] = []
        rows = self._run_sql(
            f"SELECT grantor, grantee, privilege_type "
            f"FROM {source}.catalog_privileges "
            f"WHERE catalog_name = '{catalog}'"
        )
        if rows:
            grouped: dict[str, list[str]] = defaultdict(list)
            for row in rows:
                grouped[row[1]].append(row[2])
            for principal, privs in grouped.items():
                catalog_grants.append(PermissionGrant(
                    principal=principal,
                    privileges=sorted(set(privs)),
                    inherited_from=catalog,
                ))

        # ── Schema-level grants ───────────────────────────────────────────
        schema_perms: dict[str, list[PermissionGrant]] = defaultdict(list)
        rows = self._run_sql(
            f"SELECT grantor, grantee, privilege_type, schema_name "
            f"FROM {source}.schema_privileges "
            f"WHERE catalog_name = '{catalog}'"
        )
        if rows:
            grouped_s: dict[tuple[str, str], list[str]] = defaultdict(list)
            for row in rows:
                grouped_s[(row[3], row[1])].append(row[2])
            for (schema_name, principal), privs in grouped_s.items():
                schema_perms[schema_name].append(PermissionGrant(
                    principal=principal,
                    privileges=sorted(set(privs)),
                    inherited_from=f"{catalog}.{schema_name}",
                ))

        # ── Table-level grants ────────────────────────────────────────────
        table_perms: dict[str, list[PermissionGrant]] = defaultdict(list)
        rows = self._run_sql(
            f"SELECT grantor, grantee, privilege_type, table_schema, table_name "
            f"FROM {source}.table_privileges "
            f"WHERE table_catalog = '{catalog}'"
        )
        if rows:
            grouped_t: dict[tuple[str, str, str], list[str]] = defaultdict(list)
            for row in rows:
                grouped_t[(row[3], row[4], row[1])].append(row[2])
            for (schema_name, table_name, principal), privs in grouped_t.items():
                table_perms[f"{schema_name}.{table_name}"].append(PermissionGrant(
                    principal=principal,
                    privileges=sorted(set(privs)),
                    inherited_from=f"{catalog}.{schema_name}.{table_name}",
                ))

        # ── Merge onto tables ─────────────────────────────────────────────
        self._merge_permissions(tables, catalog_grants, schema_perms, table_perms)

    def _merge_permissions(
        self,
        tables: list[TableInfo],
        catalog_grants: list[PermissionGrant],
        schema_perms: dict[str, list[PermissionGrant]],
        table_perms: dict[str, list[PermissionGrant]],
    ):
        """Merge catalog, schema, and table grants onto each table."""
        for table in tables:
            merged: dict[str, PermissionGrant] = {}

            for g in catalog_grants:
                merged[g.principal] = PermissionGrant(
                    principal=g.principal,
                    privileges=list(g.privileges),
                    inherited_from=g.inherited_from,
                )

            for g in schema_perms.get(table.schema, []):
                if g.principal in merged:
                    existing = set(merged[g.principal].privileges)
                    existing.update(g.privileges)
                    merged[g.principal].privileges = sorted(existing)
                    merged[g.principal].inherited_from = g.inherited_from
                else:
                    merged[g.principal] = PermissionGrant(
                        principal=g.principal,
                        privileges=list(g.privileges),
                        inherited_from=g.inherited_from,
                    )

            tkey = f"{table.schema}.{table.name}"
            for g in table_perms.get(tkey, []):
                if g.principal in merged:
                    existing = set(merged[g.principal].privileges)
                    existing.update(g.privileges)
                    merged[g.principal].privileges = sorted(existing)
                    merged[g.principal].inherited_from = g.inherited_from
                else:
                    merged[g.principal] = PermissionGrant(
                        principal=g.principal,
                        privileges=list(g.privileges),
                        inherited_from=g.inherited_from,
                    )

            table.permissions = list(merged.values())

    def _fetch_row_counts(self, tables: list[TableInfo]):
        """Fetch row counts for tables missing them via SQL against actual tables."""
        needing = [t for t in tables if t.row_count is None]
        for table in needing:
            rows = self._run_sql(f"SELECT count(*) as cnt FROM {table.full_name}")
            if rows:
                try:
                    table.row_count = int(rows[0][0])
                except (ValueError, TypeError, IndexError):
                    pass

    def get_schemas(self, catalog: str | None = None) -> list[dict]:
        schemas = self._schemas
        if catalog:
            schemas = [s for s in schemas if s.catalog == catalog]
        return [
            {
                "catalog": s.catalog,
                "name": s.name,
                "full_name": s.full_name,
                "table_count": s.table_count,
                "owner": s.owner,
                "comment": s.comment,
            }
            for s in schemas
        ]

    def get_tables(self, schema: str | None = None, catalog: str | None = None) -> list[dict]:
        tables = self._tables
        if catalog:
            tables = [t for t in tables if t.catalog == catalog]
        if schema:
            tables = [t for t in tables if t.schema == schema]
        return [t.to_dict() for t in tables]

    def get_table_by_full_name(self, catalog: str, schema: str, table: str) -> dict | None:
        for t in self._tables:
            if t.catalog == catalog and t.schema == schema and t.name == table:
                return t.to_dict()
        return None

    def get_table_raw(self, catalog: str, schema: str, table: str) -> TableInfo | None:
        for t in self._tables:
            if t.catalog == catalog and t.schema == schema and t.name == table:
                return t
        return None

    def get_all_tables_raw(self) -> list[TableInfo]:
        return self._tables


scanner = CatalogScanner()

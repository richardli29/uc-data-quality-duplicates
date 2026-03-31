"""Unity Catalog metadata scanner. Collects table/column info and permissions.

Permissions are read from materialized views in a ``governance`` schema within the
scanned catalog (mirrors of system.information_schema).  This avoids needing
MANAGE or metastore-admin — only SELECT on the governance schema is required.

Falls back to system.information_schema if governance MVs don't exist, and
degrades gracefully if neither source is accessible."""

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
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
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

    def list_catalogs(self) -> list[dict]:
        """Return all accessible catalogs, filtering out system/sharing catalogs."""
        skip_names = {"system", "samples", "__databricks_internal"}
        skip_types = {"SYSTEM_CATALOG", "DELTASHARING_CATALOG"}
        result = []
        for c in self.client.catalogs.list():
            if c.name in skip_names:
                continue
            ctype = getattr(c, "catalog_type", None)
            if ctype and str(ctype) in skip_types:
                continue
            if hasattr(ctype, "value") and ctype.value in skip_types:
                continue
            result.append({
                "name": c.name,
                "owner": c.owner,
                "comment": c.comment,
            })
        return result

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
        """Scan a single catalog and append results to the shared lists."""
        logger.info(f"Scanning catalog: {catalog}")
        local_tables: list[TableInfo] = []
        local_schemas: list[SchemaInfo] = []

        schemas = list(self.client.schemas.list(catalog_name=catalog))
        for s in schemas:
            if s.name == "information_schema":
                continue
            schema_info = SchemaInfo(
                catalog=catalog,
                name=s.name,
                full_name=s.full_name,
                owner=s.owner,
                comment=s.comment,
            )

            tables = list(self.client.tables.list(
                catalog_name=catalog, schema_name=s.name
            ))
            schema_info.table_count = len(tables)
            local_schemas.append(schema_info)

            for t in tables:
                cols = []
                if t.columns:
                    for c in t.columns:
                        tn = c.type_name or c.type_text or "unknown"
                        if hasattr(tn, 'value'):
                            tn = tn.value
                        cols.append(ColumnInfo(
                            name=c.name,
                            type_name=str(tn),
                            position=c.position or 0,
                            comment=c.comment,
                            nullable=c.nullable if c.nullable is not None else True,
                        ))

                row_count = None
                if t.properties:
                    for key in ("numRows", "spark.sql.statistics.numRows"):
                        if key in t.properties:
                            try:
                                row_count = int(t.properties[key])
                            except (ValueError, TypeError):
                                pass
                            break

                local_tables.append(TableInfo(
                    catalog=catalog,
                    schema=s.name,
                    name=t.name,
                    full_name=t.full_name,
                    table_type=t.table_type.value if t.table_type else "UNKNOWN",
                    columns=cols,
                    row_count=row_count,
                    created_at=t.created_at,
                    updated_at=t.updated_at,
                    owner=t.owner,
                    comment=t.comment,
                ))

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

    def _fetch_permissions(self, catalog: str, tables: list[TableInfo]):
        """Fetch permissions from governance MVs, falling back to system.information_schema.

        Tries ``<catalog>.governance.catalog_privileges`` etc. first (materialized
        views that mirror system.information_schema — only needs SELECT on the
        governance schema).  Falls back to ``system.information_schema`` if the
        governance schema doesn't exist, and silently skips if neither is accessible.
        """
        schema_perms: dict[str, list[PermissionGrant]] = defaultdict(list)
        table_perms: dict[str, list[PermissionGrant]] = defaultdict(list)
        catalog_grants: list[PermissionGrant] = []

        gov = f"{catalog}.governance"
        sys_is = "system.information_schema"
        test = self._run_sql(f"SELECT 1 FROM {gov}.catalog_privileges LIMIT 1")
        source = gov if test is not None else sys_is
        logger.info(f"Permissions source for {catalog}: {source}")

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
        else:
            logger.info(f"catalog_privileges returned no rows for {catalog}")

        rows = self._run_sql(
            f"SELECT grantor, grantee, privilege_type, schema_name "
            f"FROM {source}.schema_privileges "
            f"WHERE catalog_name = '{catalog}'"
        )
        if rows:
            grouped_schema: dict[tuple[str, str], list[str]] = defaultdict(list)
            for row in rows:
                grouped_schema[(row[3], row[1])].append(row[2])
            for (schema_name, principal), privs in grouped_schema.items():
                schema_perms[schema_name].append(PermissionGrant(
                    principal=principal,
                    privileges=sorted(set(privs)),
                    inherited_from=f"{catalog}.{schema_name}",
                ))

        rows = self._run_sql(
            f"SELECT grantor, grantee, privilege_type, table_schema, table_name "
            f"FROM {source}.table_privileges "
            f"WHERE table_catalog = '{catalog}'"
        )
        if rows:
            grouped_table: dict[tuple[str, str, str], list[str]] = defaultdict(list)
            for row in rows:
                grouped_table[(row[3], row[4], row[1])].append(row[2])
            for (schema_name, table_name, principal), privs in grouped_table.items():
                table_perms[f"{schema_name}.{table_name}"].append(PermissionGrant(
                    principal=principal,
                    privileges=sorted(set(privs)),
                    inherited_from=f"{catalog}.{schema_name}.{table_name}",
                ))

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
        """Fetch row counts for tables missing them via SQL."""
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

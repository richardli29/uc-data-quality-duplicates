"""Unity Catalog metadata scanner. Collects table/column info and effective permissions."""

from __future__ import annotations
import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional

from server.config import get_workspace_client, CATALOG_NAME, WAREHOUSE_ID

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

    def scan(self, catalog: str = CATALOG_NAME) -> dict:
        logger.info(f"Scanning catalog: {catalog}")
        self._tables = []
        self._schemas = []

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
            self._schemas.append(schema_info)

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

                table_info = TableInfo(
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
                )
                self._tables.append(table_info)

        self._fetch_permissions(catalog)
        self._fetch_row_counts(catalog)
        self._scanned = True

        return {
            "catalog": catalog,
            "schema_count": len(self._schemas),
            "table_count": len(self._tables),
            "column_count": sum(len(t.columns) for t in self._tables),
        }

    def _fetch_permissions(self, catalog: str) -> list[str]:
        """Fetch permissions for schemas (inherited to all tables) and merge."""
        errors = []
        schema_perms: dict[str, list[PermissionGrant]] = {}

        def _fetch_grants(url: str) -> tuple[dict, str | None]:
            """Fetch grants from a UC permissions URL, handling various SDK return types."""
            try:
                raw = self.client.api_client.do("GET", url)
                # Handle different return types from SDK versions
                if isinstance(raw, dict):
                    return raw, None
                if isinstance(raw, (str, bytes)):
                    text = raw if isinstance(raw, str) else raw.decode()
                    return json.loads(text), None
                # Might be a response object
                if hasattr(raw, 'json'):
                    return raw.json(), None
                if hasattr(raw, 'text'):
                    return json.loads(raw.text), None
                if hasattr(raw, 'read'):
                    return json.loads(raw.read()), None
                return {}, f"unknown type: {type(raw).__name__}: {str(raw)[:80]}"
            except Exception as e:
                return {}, str(e)[:120]

        def _extract_privs(priv_list: list) -> list[str]:
            """Extract privilege strings from varying API formats."""
            result = []
            for p in priv_list:
                if isinstance(p, str):
                    result.append(p)
                elif isinstance(p, dict):
                    result.append(p.get("privilege", str(p)))
                else:
                    result.append(str(p))
            return result

        for schema_info in self._schemas:
            data, err = _fetch_grants(
                f"/api/2.1/unity-catalog/permissions/schema/{catalog}.{schema_info.name}"
            )
            if err:
                errors.append(f"schema {schema_info.name}: {err}")
                continue
            grants = []
            for pa in data.get("privilege_assignments", []):
                privs = _extract_privs(pa.get("privileges", []))
                grants.append(PermissionGrant(
                    principal=pa.get("principal", ""),
                    privileges=privs,
                    inherited_from=f"{catalog}.{schema_info.name}",
                ))
            schema_perms[schema_info.name] = grants

        for table in self._tables:
            if table.schema in schema_perms:
                table.permissions = list(schema_perms[table.schema])

        data, err = _fetch_grants(f"/api/2.1/unity-catalog/permissions/catalog/{catalog}")
        if err:
            errors.append(f"catalog: {err}")
        else:
            catalog_grants = []
            for pa in data.get("privilege_assignments", []):
                privs = _extract_privs(pa.get("privileges", []))
                catalog_grants.append(PermissionGrant(
                    principal=pa.get("principal", ""),
                    privileges=privs,
                    inherited_from=catalog,
                ))
            for table in self._tables:
                for cg in catalog_grants:
                    if not any(p.principal == cg.principal for p in table.permissions):
                        table.permissions.append(cg)

        return errors

    def _fetch_row_counts(self, catalog: str):
        """Fetch row counts for tables missing them via SQL using the SDK's API client."""
        tables_needing_counts = [t for t in self._tables if t.row_count is None]
        if not tables_needing_counts:
            return

        for table in tables_needing_counts:
            try:
                sql = f"SELECT count(*) as cnt FROM {table.full_name}"
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
                    chunks = data.get("result", {}).get("data_array", [])
                    if chunks:
                        table.row_count = int(chunks[0][0])
            except Exception as e:
                logger.debug(f"Row count query failed for {table.full_name}: {e}")

    def get_schemas(self) -> list[dict]:
        return [
            {
                "catalog": s.catalog,
                "name": s.name,
                "full_name": s.full_name,
                "table_count": s.table_count,
                "owner": s.owner,
                "comment": s.comment,
            }
            for s in self._schemas
        ]

    def get_tables(self, schema: str | None = None) -> list[dict]:
        tables = self._tables
        if schema:
            tables = [t for t in tables if t.schema == schema]
        return [t.to_dict() for t in tables]

    def get_table(self, schema: str, table: str) -> dict | None:
        for t in self._tables:
            if t.schema == schema and t.name == table:
                return t.to_dict()
        return None

    def get_all_tables_raw(self) -> list[TableInfo]:
        return self._tables


scanner = CatalogScanner()

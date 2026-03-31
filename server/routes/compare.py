import traceback
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from server.scanner import scanner, TableInfo
from server.comparator import compare_tables, fetch_sample_data
from server.config import CATALOG_NAME

router = APIRouter(prefix="/api/compare", tags=["compare"])


def _resolve_catalog(catalog: str | None) -> str:
    return catalog or scanner.current_catalog or CATALOG_NAME


def _get_table_info(schema: str, table: str) -> TableInfo:
    for t in scanner.get_all_tables_raw():
        if t.schema == schema and t.name == table:
            return t
    raise HTTPException(status_code=404, detail=f"Table {schema}.{table} not found")


@router.get("/{schema1}/{table1}/{schema2}/{table2}")
def compare(
    schema1: str, table1: str, schema2: str, table2: str,
    catalog: str | None = Query(None),
):
    try:
        cat = _resolve_catalog(catalog)
        if scanner.needs_scan(cat):
            scanner.scan(cat)
        ta = _get_table_info(schema1, table1)
        tb = _get_table_info(schema2, table2)
        return compare_tables(ta, tb)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()},
        )


@router.get("/sample/{schema}/{table}")
def sample(schema: str, table: str, catalog: str | None = Query(None)):
    try:
        cat = _resolve_catalog(catalog)
        if scanner.needs_scan(cat):
            scanner.scan(cat)
        t = _get_table_info(schema, table)
        result = fetch_sample_data(t.full_name)
        if result is None:
            raise HTTPException(status_code=500, detail="Could not fetch sample data")
        return result
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()},
        )

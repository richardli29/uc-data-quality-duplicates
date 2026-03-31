import traceback
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from server.scanner import scanner, TableInfo
from server.comparator import compare_tables, fetch_sample_data

router = APIRouter(prefix="/api/compare", tags=["compare"])


def _get_table_info(catalog: str, schema: str, table: str) -> TableInfo:
    t = scanner.get_table_raw(catalog, schema, table)
    if t is None:
        raise HTTPException(status_code=404, detail=f"Table {catalog}.{schema}.{table} not found")
    return t


@router.get("/{cat1}/{schema1}/{table1}/{cat2}/{schema2}/{table2}")
def compare(cat1: str, schema1: str, table1: str, cat2: str, schema2: str, table2: str):
    try:
        if not scanner.is_scanned:
            scanner.scan_all()
        ta = _get_table_info(cat1, schema1, table1)
        tb = _get_table_info(cat2, schema2, table2)
        return compare_tables(ta, tb)
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()},
        )


@router.get("/sample/{catalog}/{schema}/{table}")
def sample(catalog: str, schema: str, table: str):
    try:
        if not scanner.is_scanned:
            scanner.scan_all()
        t = _get_table_info(catalog, schema, table)
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

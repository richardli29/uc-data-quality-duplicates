import traceback
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from server.scanner import scanner

router = APIRouter(prefix="/api/catalog", tags=["catalog"])


@router.get("/list")
def list_catalogs():
    try:
        scanner.reset_client()
        return scanner.list_catalogs()
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()},
        )


@router.get("/scan-all")
def scan_all():
    try:
        return scanner.scan_all()
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()},
        )


@router.get("/schemas")
def list_schemas(catalog: str | None = Query(None)):
    if not scanner.is_scanned:
        return []
    return scanner.get_schemas(catalog)


@router.get("/tables")
def list_tables(schema: str | None = None, catalog: str | None = Query(None)):
    if not scanner.is_scanned:
        return []
    return scanner.get_tables(schema, catalog)


@router.get("/table/{catalog}/{schema}/{table}")
def get_table(catalog: str, schema: str, table: str):
    if not scanner.is_scanned:
        raise HTTPException(status_code=400, detail="No scan has been run yet")
    result = scanner.get_table_by_full_name(catalog, schema, table)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Table {catalog}.{schema}.{table} not found")
    return result

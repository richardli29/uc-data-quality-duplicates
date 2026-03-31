import traceback
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from server.scanner import scanner
from server.config import CATALOG_NAME

router = APIRouter(prefix="/api/catalog", tags=["catalog"])


def _resolve_catalog(catalog: str | None) -> str:
    return catalog or scanner.current_catalog or CATALOG_NAME


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


@router.get("/scan")
def scan_catalog(catalog: str | None = Query(None)):
    try:
        cat = _resolve_catalog(catalog)
        scanner.reset_client()
        return scanner.scan(cat)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()},
        )


@router.get("/schemas")
def list_schemas(catalog: str | None = Query(None)):
    cat = _resolve_catalog(catalog)
    if scanner.needs_scan(cat):
        scanner.scan(cat)
    return scanner.get_schemas()


@router.get("/tables")
def list_tables(schema: str | None = None, catalog: str | None = Query(None)):
    cat = _resolve_catalog(catalog)
    if scanner.needs_scan(cat):
        scanner.scan(cat)
    return scanner.get_tables(schema)


@router.get("/table/{schema}/{table}")
def get_table(schema: str, table: str, catalog: str | None = Query(None)):
    cat = _resolve_catalog(catalog)
    if scanner.needs_scan(cat):
        scanner.scan(cat)
    result = scanner.get_table(schema, table)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Table {schema}.{table} not found")
    return result

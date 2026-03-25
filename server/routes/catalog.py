import traceback
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from server.scanner import scanner
from server.config import CATALOG_NAME

router = APIRouter(prefix="/api/catalog", tags=["catalog"])


@router.get("/scan")
def scan_catalog():
    try:
        scanner.reset_client()
        return scanner.scan(CATALOG_NAME)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()},
        )


@router.get("/schemas")
def list_schemas():
    if not scanner.is_scanned:
        scanner.scan(CATALOG_NAME)
    return scanner.get_schemas()


@router.get("/tables")
def list_tables(schema: str | None = None):
    if not scanner.is_scanned:
        scanner.scan(CATALOG_NAME)
    return scanner.get_tables(schema)


@router.get("/table/{schema}/{table}")
def get_table(schema: str, table: str):
    if not scanner.is_scanned:
        scanner.scan(CATALOG_NAME)
    result = scanner.get_table(schema, table)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Table {schema}.{table} not found")
    return result

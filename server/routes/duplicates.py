import traceback
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from server.scanner import scanner
from server.duplicates import detect_duplicates
from server.config import CATALOG_NAME

router = APIRouter(prefix="/api/duplicates", tags=["duplicates"])

_cached_groups: list | None = None


def _resolve_catalog(catalog: str | None) -> str:
    return catalog or scanner.current_catalog or CATALOG_NAME


@router.get("/detect")
def detect(
    threshold: float = Query(0.5, ge=0.1, le=1.0),
    catalog: str | None = Query(None),
):
    global _cached_groups
    try:
        cat = _resolve_catalog(catalog)
        if scanner.needs_scan(cat):
            scanner.scan(cat)
        tables = scanner.get_all_tables_raw()
        groups = detect_duplicates(tables, threshold=threshold)
        _cached_groups = groups
        return [g.to_dict() for g in groups]
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": traceback.format_exc()},
        )


@router.get("/groups")
def get_groups():
    if _cached_groups is None:
        return detect()
    return [g.to_dict() for g in _cached_groups]

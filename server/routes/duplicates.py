import traceback
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from server.scanner import scanner
from server.duplicates import detect_duplicates
from server.config import CATALOG_NAME

router = APIRouter(prefix="/api/duplicates", tags=["duplicates"])

_cached_groups: list | None = None


@router.get("/detect")
def detect(threshold: float = Query(0.5, ge=0.1, le=1.0)):
    global _cached_groups
    try:
        if not scanner.is_scanned:
            scanner.scan(CATALOG_NAME)
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

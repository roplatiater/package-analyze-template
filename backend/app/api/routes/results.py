from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from app.core.auth import PERMISSION_READ, require_permission
from app.services.result_service import ResultService
router=APIRouter()
@router.get("", dependencies=[require_permission(PERMISSION_READ)])
async def list_results(limit: int = 20): return ResultService().list(limit)
@router.get("/{run_id}/download", dependencies=[require_permission(PERMISSION_READ)])
async def download_result(run_id: str):
    located = ResultService().locate_full_report(run_id)
    if not located: raise HTTPException(404, "full report unavailable")
    path = located["path"]; compression = located["entry"].get("compression", "none")
    media_type = "application/gzip" if compression == "gzip" else "application/json"
    filename = f"{run_id}-result.json.gz" if compression == "gzip" else f"{run_id}-result.json"
    return FileResponse(path, media_type=media_type, filename=filename)
@router.get("/{run_id}", dependencies=[require_permission(PERMISSION_READ)])
async def get_result(run_id: str): return ResultService().get(run_id)

from fastapi import APIRouter
from app.core.auth import PERMISSION_READ, require_permission
from app.services.result_service import ResultService
router=APIRouter(); svc=ResultService()
@router.get("", dependencies=[require_permission(PERMISSION_READ)])
async def list_results(limit: int = 20): return svc.list(limit)
@router.get("/{run_id}", dependencies=[require_permission(PERMISSION_READ)])
async def get_result(run_id: str): return svc.get(run_id)

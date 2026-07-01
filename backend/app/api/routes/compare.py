from fastapi import APIRouter

from app.core.auth import PERMISSION_READ, require_permission
from app.services.compare_service import CompareService


router = APIRouter()
svc = CompareService()


@router.post("", dependencies=[require_permission(PERMISSION_READ)])
async def compare_runs(body: dict | None = None):
    body = body or {}
    return svc.compare(body.get("left_run_id"), body.get("right_run_id"))

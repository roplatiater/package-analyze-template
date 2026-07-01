from fastapi import APIRouter
from app.core.auth import PERMISSION_OPS, require_permission
from app.services.metrics_service import metrics_snapshot

router = APIRouter()

@router.get("", dependencies=[require_permission(PERMISSION_OPS)])
async def metrics():
    return metrics_snapshot()

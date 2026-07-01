from fastapi import APIRouter
from app.core.auth import PERMISSION_OPS, require_permission
from app.repositories.artifact_store import ArtifactStore
router=APIRouter()
@router.get("/status", dependencies=[require_permission(PERMISSION_OPS)])
async def status(): return ArtifactStore().cache_status()

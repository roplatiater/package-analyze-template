from fastapi import APIRouter
from app.api.models import CreateJobRequest
from app.core.auth import PERMISSION_READ, PERMISSION_WRITE, require_permission
from app.services.job_service import JobService
from app.repositories.run_store import RunStore

router=APIRouter(); svc=JobService(); runs=RunStore()
@router.post("", dependencies=[require_permission(PERMISSION_WRITE)])
async def create_job(body: CreateJobRequest | None = None): return svc.create(body)
@router.get("", dependencies=[require_permission(PERMISSION_READ)])
async def list_jobs(): return {"jobs": runs.list_runs()}
@router.get("/{run_id}", dependencies=[require_permission(PERMISSION_READ)])
async def get_job(run_id: str): return runs.get_run(run_id)
@router.post("/{run_id}/retry", dependencies=[require_permission(PERMISSION_WRITE)])
async def retry_job(run_id: str): return svc.retry(run_id)

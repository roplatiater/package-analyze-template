import asyncio
from datetime import datetime, timezone, timedelta

from app.core.config import settings
from app.pipeline.models import PipelineStatus, StepStatus, now_iso
from app.repositories.run_store import RunStore
from app.services.job_service import JobService


def has_active_scheduled_job() -> bool:
    runs = RunStore()
    for p in runs.list_runs():
        if p.get("request", {}).get("trigger") != "scheduled":
            continue
        if p.get("status") in (PipelineStatus.queued, PipelineStatus.running):
            return True
        try:
            run = runs.get_run(p["run_id"])
        except Exception:
            continue
        if any(s.get("status") in (StepStatus.ready, StepStatus.running, StepStatus.retrying) for s in run["steps"]):
            return True
    return False


def scheduled_body(now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    interval = settings.auto_pipeline_interval_seconds
    aligned = int(now.timestamp()) // interval * interval
    start = datetime.fromtimestamp(aligned, timezone.utc)
    end = start + timedelta(seconds=interval)
    return {
        "source": settings.default_source,
        "dataset": settings.default_dataset,
        "analysis_type": settings.default_analysis_type,
        "trigger": "scheduled",
        "requested_at": now_iso(),
        "params": {"batch_id": start.strftime("%Y%m%d_%H%M%S"), "window_start": start.isoformat(), "window_end": end.isoformat()},
    }


def trigger_once() -> dict | None:
    if has_active_scheduled_job():
        return None
    return JobService().create(scheduled_body())


async def run_periodic_trigger():
    try:
        trigger_once()
        while True:
            await asyncio.sleep(settings.auto_pipeline_interval_seconds)
            trigger_once()
    except asyncio.CancelledError:
        raise

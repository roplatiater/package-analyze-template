from collections import Counter
from app.core.config import settings
from app.repositories.run_store import RunStore


def metrics_snapshot():
    runs = RunStore()
    job_status = Counter(); step_status = Counter(); step_type_status = Counter(); events = Counter()
    for pipeline in runs.list_runs():
        job_status[str(pipeline.get("status"))] += 1
        try:
            for step in runs.get_run(pipeline["run_id"])["steps"]:
                st = str(step.get("status")); typ = str(step.get("step_type"))
                step_status[st] += 1; step_type_status[f"{typ}:{st}"] += 1
        except Exception:
            continue
    if hasattr(runs, "event_counts"):
        events.update(runs.event_counts())
    return {
        "metadata_store": settings.metadata_store,
        "jobs_by_status": dict(job_status),
        "steps_by_status": dict(step_status),
        "steps_by_type_status": dict(step_type_status),
        "queued_steps": step_status.get("ready", 0) + step_status.get("retrying", 0),
        "running_steps": step_status.get("running", 0),
        "events_by_type": dict(events),
    }

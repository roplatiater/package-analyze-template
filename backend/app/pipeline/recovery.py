from datetime import datetime, timezone
from pathlib import Path
from app.pipeline.models import PipelineStatus, StepStatus, now_iso
from app.repositories.artifact_store import ArtifactStore
from app.repositories.run_store import RunStore

OK=(StepStatus.succeeded, StepStatus.succeeded_cached)
INVALID_OUTPUT_ERRORS={"MISSING_OR_INVALID_OUTPUT_MANIFEST","LEASE_EXPIRED"}

async def recover(clean_tmp: bool = False):
    runs=RunStore(); artifacts=ArtifactStore()
    if clean_tmp: artifacts.clean_tmp()
    if hasattr(runs, "recover_expired_leases"):
        runs.recover_expired_leases()
        return
    now=datetime.now(timezone.utc)
    for run_id in runs.list_ids():
        try: steps=runs.get_run(run_id)["steps"]
        except Exception: continue
        by={s["step_id"]:s for s in steps}
        def valid(s):
            mp=s.get("output_manifest_path") or s.get("expected_output_manifest_path")
            if not mp: return False
            return bool(artifacts.valid_manifest(Path(mp), s.get("output_artifact_type"), s.get("filter"), s.get("filter_version"), s.get("output_cache_key")))
        def dep_state(dep):
            if dep["status"] in (StepStatus.failed, StepStatus.blocked): return "invalid"
            if dep.get("error") in INVALID_OUTPUT_ERRORS: return "invalid"
            if dep["status"] in OK: return "valid" if valid(dep) else "invalid"
            return "unavailable"
        for s in steps:
            changed=False
            if s["status"]==StepStatus.running and (not s.get("lease_until") or datetime.fromisoformat(s["lease_until"]) <= now):
                if valid(s): s.update(status=StepStatus.succeeded_cached, output_manifest_path=s.get("output_manifest_path") or s.get("expected_output_manifest_path"))
                elif s.get("attempt",0) < s.get("max_attempts",1): s.update(status=StepStatus.ready, worker_id=None, lease_until=None, heartbeat_at=None, finished_at=None, error="MISSING_OR_INVALID_OUTPUT_MANIFEST")
                else: s.update(status=StepStatus.failed, error="LEASE_EXPIRED")
                changed=True
            if s["status"]==StepStatus.retrying and s.get("next_retry_at") and datetime.fromisoformat(s["next_retry_at"]) <= now:
                s.update(status=StepStatus.ready); changed=True
            if s["status"] in OK and not valid(s):
                s.update(status=StepStatus.ready if s.get("attempt",0)<s.get("max_attempts",1) else StepStatus.failed, error="MISSING_OR_INVALID_OUTPUT_MANIFEST", worker_id=None, lease_until=None, heartbeat_at=None, finished_at=None); changed=True
            if changed: runs.save_step(run_id,s["step_id"],s)
        steps=runs.get_run(run_id)["steps"]; by={s["step_id"]:s for s in steps}
        for s in steps:
            if s.get("depends_on") and s["status"] not in (StepStatus.failed,):
                states=[dep_state(by[d]) for d in s.get("depends_on",[])]
                if "invalid" in states:
                    s.update(status=StepStatus.blocked,error="DEPENDENCY_INVALID"); runs.save_step(run_id,s["step_id"],s)
                    by[s["step_id"]]=s
                elif all(st=="valid" for st in states) and s["status"] in (StepStatus.pending, StepStatus.blocked, StepStatus.ready):
                    s.update(status=StepStatus.ready,error=None,worker_id=None,lease_until=None,heartbeat_at=None); runs.save_step(run_id,s["step_id"],s)
                    by[s["step_id"]]=s
                elif any(st=="unavailable" for st in states) and s["status"] in (StepStatus.ready, StepStatus.running, StepStatus.retrying, StepStatus.blocked):
                    s.update(status=StepStatus.pending,error=None,worker_id=None,lease_until=None,heartbeat_at=None); runs.save_step(run_id,s["step_id"],s)
                    by[s["step_id"]]=s
        steps=runs.get_run(run_id)["steps"]; statuses={s["status"] for s in steps}; p=runs.get_pipeline(run_id)
        final=next((s for s in steps if s["step_id"]==p.get("final_step_id")), steps[-1])
        p["status"] = PipelineStatus.succeeded if final["status"] in OK else PipelineStatus.failed if StepStatus.failed in statuses else PipelineStatus.blocked if StepStatus.blocked in statuses else PipelineStatus.queued
        p["updated_at"]=now_iso(); runs.save_pipeline(run_id,p)

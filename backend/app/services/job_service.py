from datetime import datetime, timezone, timedelta
from pathlib import Path
from app.core.config import settings
from app.pipeline.models import PipelineStatus, StepStatus, now_iso
from app.repositories.artifact_store import ArtifactStore
from app.repositories.run_store import RunStore
from fastapi import HTTPException
from pydantic import BaseModel

DEFAULT = {"source":settings.default_source, "dataset":settings.default_dataset, "analysis_type":settings.default_analysis_type, "params":{}}

def default_window(trigger: str):
    now = datetime.now(timezone.utc)
    start = now - timedelta(seconds=settings.auto_pipeline_interval_seconds)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    return {"batch_id": f"manual_{stamp}" if trigger == "manual" else stamp, "window_start": start.isoformat(), "window_end": now.isoformat()}

class JobService:
    def __init__(self): self.runs=RunStore()
    def _valid_step_manifest(self, artifacts, s):
        if not s.get("output_manifest_path"): return False
        return bool(artifacts.valid_manifest(Path(s["output_manifest_path"]), s.get("output_artifact_type"), s.get("filter"), s.get("filter_version"), s.get("output_cache_key")))
    def _promote_by_dependencies(self, run_id: str):
        artifacts=ArtifactStore(); steps={s["step_id"]:s for s in self.runs.get_run(run_id)["steps"]}
        for s in steps.values():
            if s["status"] in (StepStatus.pending, StepStatus.blocked, StepStatus.retrying, StepStatus.ready) and s.get("depends_on"):
                deps=[steps[d] for d in s["depends_on"]]
                if all(d["status"] in (StepStatus.succeeded, StepStatus.succeeded_cached) and self._valid_step_manifest(artifacts,d) for d in deps):
                    s.update(status=StepStatus.ready,error=None,worker_id=None,lease_until=None,heartbeat_at=None)
                    self.runs.save_step(run_id,s["step_id"],s)
    def create(self, body: dict | BaseModel | None):
        if isinstance(body, BaseModel):
            body = body.model_dump(exclude_none=True)
        body = body or {}; trigger = body.get("trigger", "manual")
        req = DEFAULT | body
        req["trigger"] = trigger; req["requested_at"] = body.get("requested_at") or now_iso()
        params = default_window(trigger) | body.get("params", {})
        req["params"] = params
        run_id = self.runs.create(req)
        if params.get("force_refresh"):
            p = self.runs.get_pipeline(run_id)
            p["request"].setdefault("params", {})["refresh_nonce"] = run_id
            self.runs.save_pipeline(run_id, p)
        return self.runs.get_run(run_id)
    def retry(self, run_id: str):
        if hasattr(self.runs, "retry_pipeline"):
            return self.runs.retry_pipeline(run_id)
        p=self.runs.get_pipeline(run_id)
        if p.get("status") not in (PipelineStatus.failed, PipelineStatus.blocked):
            raise HTTPException(409, "only failed/blocked jobs can be retried")
        p["status"]=PipelineStatus.queued; p["updated_at"]=now_iso(); p["request"].setdefault("params",{}).pop("simulate_fail_stage", None); self.runs.save_pipeline(run_id,p)
        for s0 in self.runs.get_run(run_id)["steps"]:
            name=s0["step_id"]
            s=self.runs.get_step(run_id,name)
            if s["status"] not in (StepStatus.succeeded, StepStatus.succeeded_cached):
                s.update(status=StepStatus.ready if not s.get("depends_on") else StepStatus.pending, attempt=0, error=None, next_retry_at=None)
                self.runs.save_step(run_id,name,s)
        self._promote_by_dependencies(run_id)
        return self.runs.get_run(run_id)

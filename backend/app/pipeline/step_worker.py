import asyncio, uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from app.core.config import settings
from app.filters.registry import registry
from app.pipeline.models import StepStatus, PipelineStatus, now_iso
from app.pipeline.recovery import recover
from app.pipeline.step_executor import StepExecutor, OK
from app.repositories.artifact_store import ArtifactStore
from app.repositories.lock_store import step_lock
from app.repositories.run_store import RunStore

class StepWorker:
    def __init__(self, step_type: str, worker_id: str | None=None): self.step_type=step_type; self.worker_id=worker_id or f"{step_type}-{uuid.uuid4().hex[:6]}"; self.runs=RunStore(); self.artifacts=ArtifactStore()
    def _set_pipeline_blocked_or_queued(self, run_id):
        p=self.runs.get_pipeline(run_id); steps=self.runs.get_run(run_id)["steps"]; statuses={x["status"] for x in steps}
        p["status"]=PipelineStatus.blocked if StepStatus.blocked in statuses else PipelineStatus.queued
        p["updated_at"]=now_iso(); self.runs.save_pipeline(run_id,p)
    def _deps(self, run_id, s):
        out=[]; steps={x["step_id"]:x for x in self.runs.get_run(run_id)["steps"]}
        invalid_errors={"MISSING_OR_INVALID_OUTPUT_MANIFEST","LEASE_EXPIRED"}
        for d in s.get("depends_on",[]):
            ds=steps[d]
            if ds["status"] in (StepStatus.failed, StepStatus.blocked) or ds.get("error") in invalid_errors: return "invalid", []
            if ds["status"] not in OK: return "unavailable", []
            if not ds.get("output_manifest_path"): return "invalid", []
            m=self.artifacts.valid_manifest(Path(ds["output_manifest_path"]), ds["output_artifact_type"], ds["filter"], ds["filter_version"], ds.get("output_cache_key"))
            if not m: return "invalid", []
            out.append({"type":ds["output_artifact_type"],"manifest_path":ds["output_manifest_path"],"manifest_hash":self.artifacts.manifest_hash(ds["output_manifest_path"])})
        return "valid", out
    def claim_one(self):
        if hasattr(self.runs, "claim_ready_step"):
            claimed=self.runs.claim_ready_step(self.step_type, self.worker_id)
            if not claimed: return None
            run_id, step_id = claimed; s=self.runs.get_step(run_id, step_id)
            dep_status, inputs=self._deps(run_id, s)
            if dep_status != "valid":
                self.runs.prepare_claimed_step(run_id, step_id, self.worker_id, s["attempt"], {"status": StepStatus.blocked if dep_status == "invalid" else StepStatus.pending, "error": "DEPENDENCY_INVALID" if dep_status == "invalid" else None, "worker_id": None, "lease_until": None, "heartbeat_at": None})
                return None
            item=registry.get(s["step_type"]); req=self.runs.get_pipeline(run_id)["request"]
            context={"run_id":run_id,"run_request":req,"step":s,"params":req.get("params",{}),"input_artifacts":inputs}
            key=item.filter_class().build_cache_key(context); target=item.output_path_builder(settings.data_root, req, key)
            ok=self.runs.prepare_claimed_step(run_id, step_id, self.worker_id, s["attempt"], {"input_artifacts": inputs, "output_cache_key": key, "expected_output_manifest_path": str(target/"manifest.json"), "error": None})
            return (run_id, step_id) if ok else None
        now=datetime.now(timezone.utc)
        for run_id in self.runs.list_ids():
            for s0 in self.runs.get_run(run_id)["steps"]:
                if s0.get("step_type")!=self.step_type: continue
                if s0["status"] not in (StepStatus.ready, StepStatus.retrying): continue
                if s0["status"]==StepStatus.retrying and s0.get("next_retry_at") and datetime.fromisoformat(s0["next_retry_at"])>now: continue
                with step_lock(run_id, s0["step_id"]):
                    s=self.runs.get_step(run_id,s0["step_id"])
                    if s["status"] not in (StepStatus.ready, StepStatus.retrying): continue
                    dep_status, inputs=self._deps(run_id,s)
                    if dep_status != "valid":
                        if dep_status == "invalid": s.update(status=StepStatus.blocked,error="DEPENDENCY_INVALID",worker_id=None,lease_until=None,heartbeat_at=None)
                        else: s.update(status=StepStatus.pending,error=None,worker_id=None,lease_until=None,heartbeat_at=None)
                        self.runs.save_step(run_id,s["step_id"],s); self._set_pipeline_blocked_or_queued(run_id); continue
                    item=registry.get(s["step_type"]); req=self.runs.get_pipeline(run_id)["request"]
                    context={"run_id":run_id,"run_request":req,"step":s,"params":req.get("params",{}),"input_artifacts":inputs}
                    key=item.filter_class().build_cache_key(context); target=item.output_path_builder(settings.data_root, req, key)
                    s.update(status=StepStatus.running, worker_id=self.worker_id, claimed_at=now_iso(), lease_until=(now+timedelta(seconds=settings.worker_lease_seconds)).isoformat(), heartbeat_at=now_iso(), attempt=s["attempt"]+1, input_artifacts=inputs, output_cache_key=key, expected_output_manifest_path=str(target/"manifest.json"), error=None)
                    self.runs.save_step(run_id,s["step_id"],s)
                    p=self.runs.get_pipeline(run_id); p["status"]=PipelineStatus.running; p["updated_at"]=now_iso(); self.runs.save_pipeline(run_id,p)
                    return run_id, s["step_id"]
        return None
    async def run_once(self):
        await recover(); claimed=self.claim_one()
        if claimed: await StepExecutor().execute(*claimed); return True
        return False
    async def run_forever(self, interval=1.0):
        while True:
            did=await self.run_once()
            if not did: await asyncio.sleep(interval)

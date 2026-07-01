import asyncio, json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from app.core.config import settings
from app.filters.registry import registry
from app.pipeline.models import PipelineStatus, StepStatus, now_iso
from app.repositories.artifact_store import ArtifactStore
from app.repositories.lock_store import artifact_lock, step_lock
from app.repositories.run_store import RunStore

OK=(StepStatus.succeeded, StepStatus.succeeded_cached)

class StepExecutor:
    def __init__(self): self.runs=RunStore(); self.artifacts=ArtifactStore()
    def _valid(self, s):
        if not s.get("output_manifest_path"): return None
        return self.artifacts.valid_manifest(Path(s["output_manifest_path"]), s["output_artifact_type"], s["filter"], s["filter_version"], s.get("output_cache_key"))
    def _update_pipeline(self, run_id):
        p=self.runs.get_pipeline(run_id); steps=self.runs.get_run(run_id)["steps"]; statuses={s["status"] for s in steps}
        final=next((s for s in steps if s["step_id"]==p.get("final_step_id")), steps[-1])
        p["status"] = PipelineStatus.succeeded if final["status"] in OK else PipelineStatus.failed if StepStatus.failed in statuses else PipelineStatus.blocked if StepStatus.blocked in statuses else PipelineStatus.running
        p["updated_at"]=now_iso(); self.runs.save_pipeline(run_id,p)
    def _promote(self, run_id):
        steps={s["step_id"]:s for s in self.runs.get_run(run_id)["steps"]}
        for s in steps.values():
            if s["status"] in (StepStatus.pending, StepStatus.blocked):
                if all(steps[d]["status"] in OK and self._valid(steps[d]) for d in s.get("depends_on",[])):
                    s.update(status=StepStatus.ready,error=None); self.runs.save_step(run_id,s["step_id"],s)
    async def _heartbeat(self, run_id: str, step_id: str, worker_id: str | None):
        while True:
            await asyncio.sleep(max(1, getattr(settings, "worker_heartbeat_interval_seconds", max(1, settings.worker_lease_seconds//3))))
            if hasattr(self.runs, "heartbeat_step"):
                s=self.runs.get_step(run_id, step_id)
                if not self.runs.heartbeat_step(run_id, step_id, worker_id, s.get("attempt")): return
                continue
            with step_lock(run_id, step_id):
                s=self.runs.get_step(run_id, step_id)
                if s.get("status") != StepStatus.running or s.get("worker_id") != worker_id: return
                now=datetime.now(timezone.utc)
                s.update(heartbeat_at=now.isoformat(), lease_until=(now+timedelta(seconds=settings.worker_lease_seconds)).isoformat())
                self.runs.save_step(run_id, step_id, s)
    async def execute(self, run_id: str, step_id: str):
        p=self.runs.get_pipeline(run_id); s=self.runs.get_step(run_id, step_id); item=registry.get(s["step_type"]); filt=item.filter_class()
        owner=s.get("worker_id"); attempt=s.get("attempt")
        context={"run_id":run_id,"run_request":p["request"],"step":s,"params":p["request"].get("params",{}),"input_artifacts":s.get("input_artifacts",[]),"cache_key":s["output_cache_key"]}
        heartbeat=asyncio.create_task(self._heartbeat(run_id, step_id, owner))
        try:
            with artifact_lock(s["output_cache_key"]):
                m=self.artifacts.valid_manifest(Path(s["expected_output_manifest_path"]), item.output_artifact_type, item.filter_name, item.filter_version, s["output_cache_key"], s["input_artifacts"][0]["manifest_hash"] if s.get("input_artifacts") else None)
                if m:
                    s.update(status=StepStatus.succeeded_cached, output_manifest_path=s["expected_output_manifest_path"], finished_at=now_iso(), error=None)
                else:
                    tmp=self.artifacts.tmp_dir(f"{run_id}_{step_id}_{s['attempt']}")
                    out=await filt.run(context,tmp); manifest=filt.build_manifest(context,out)
                    if context["params"].get("simulate_fail_stage")=="commit": raise RuntimeError("simulated commit failure")
                    mp=self.artifacts.commit(tmp, Path(s["expected_output_manifest_path"]).parent, manifest, item.output_artifact_type, item.filter_name, item.filter_version)
                    s.update(status=StepStatus.succeeded, output_manifest_path=mp, finished_at=now_iso(), error=None)
                if hasattr(self.runs, "finish_step"):
                    self.runs.finish_step(run_id, step_id, owner, attempt, s["output_manifest_path"], s["status"])
                else:
                    self.runs.save_step(run_id, step_id, s)
        except Exception as e:
            st=StepStatus.retrying if s["attempt"]<s["max_attempts"] else StepStatus.failed
            nr=(datetime.now(timezone.utc)+timedelta(seconds=1)).isoformat() if st==StepStatus.retrying else None
            s.update(status=st,error=str(e),next_retry_at=nr,finished_at=now_iso() if st==StepStatus.failed else None, worker_id=None, lease_until=None)
            if hasattr(self.runs, "fail_or_retry_step"):
                self.runs.fail_or_retry_step(run_id, step_id, owner, attempt, str(e), nr)
            else:
                self.runs.save_step(run_id, step_id, s)
        finally:
            heartbeat.cancel()
            try: await heartbeat
            except asyncio.CancelledError: pass
        if not hasattr(self.runs, "finish_step"):
            self._promote(run_id); self._update_pipeline(run_id)

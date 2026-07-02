import json
from pathlib import Path
from fastapi import HTTPException
from app.core.config import settings
from app.pipeline.models import StepStatus
from app.repositories.artifact_store import ArtifactStore, file_sha, read_json_artifact, resolve_artifact_file
from app.repositories.run_store import RunStore

class ResultService:
    def __init__(self): self.runs=RunStore(); self.artifacts=ArtifactStore(); self.data_root=settings.data_root.resolve()
    def list(self, limit: int = 20):
        items = []
        artifacts = ArtifactStore()
        for p in self.runs.list_runs():
            try:
                run = self.runs.get_run(p["run_id"]); s = run["steps"][-1]
                if s["status"] not in (StepStatus.succeeded, StepStatus.succeeded_cached):
                    continue
                mp = Path(s["output_manifest_path"])
                m = artifacts.read_manifest_metadata(mp,s["output_artifact_type"],s["filter"],s["filter_version"],s.get("output_cache_key"))
                if not m: continue
                data = read_json_artifact(mp, m["files"][0])
                req = p.get("request", {})
                items.append({
                    "run_id": p["run_id"], "trigger": req.get("trigger", "manual"),
                    "batch_id": req.get("params", {}).get("batch_id"), "created_at": p.get("created_at"),
                    "updated_at": p.get("updated_at"), "finished_at": s.get("finished_at"),
                    "summary": data.get("summary"), "status": p.get("status"),
                })
            except Exception:
                continue
        items.sort(key=lambda x: x.get("finished_at") or x.get("updated_at") or "", reverse=True)
        return {"items": items[:limit]}

    def get(self, run_id: str):
        self.runs.get_pipeline(run_id)
        s=self.runs.get_run(run_id)["steps"][-1]
        if s["status"] not in (StepStatus.succeeded, StepStatus.succeeded_cached): raise HTTPException(409, {"status":"not_ready"})
        mp=Path(s["output_manifest_path"])
        m=self.artifacts.read_manifest_metadata(mp,s["output_artifact_type"],s["filter"],s["filter_version"],s.get("output_cache_key"))
        if not m: raise HTTPException(409, "result manifest invalid")
        try:
            data = read_json_artifact(mp, m["files"][0])
            full = self.locate_full_report(run_id)
            return {"summary":data.get("summary",{}),"chart_data":data.get("chart_data",[]),"records_preview":data.get("records_preview",[]),"source_result_manifest":data.get("source_result_manifest"),"full_report_artifact":full.get("metadata") if full else None,"download_url":f"/api/results/{run_id}/download" if full else None}
        except Exception as e: raise HTTPException(409, f"result file invalid: {e}")

    def _safe_manifest_path(self, value: str | None) -> Path:
        if not value: raise HTTPException(409, "manifest path missing")
        try:
            p=Path(value).resolve(); p.relative_to(self.data_root); return p
        except Exception:
            raise HTTPException(409, "manifest path invalid")

    def locate_full_report(self, run_id: str):
        run=self.runs.get_run(run_id)
        final_step_id = run.get("pipeline", {}).get("final_step_id")
        s=next((step for step in run.get("steps", []) if step.get("step_id") == final_step_id), run["steps"][-1])
        if s.get("status") not in (StepStatus.succeeded, StepStatus.succeeded_cached):
            raise HTTPException(409, {"status":"not_ready"})
        mp=self._safe_manifest_path(s.get("output_manifest_path"))
        sm=self.artifacts.read_manifest_metadata(mp,s.get("output_artifact_type"),s.get("filter"),s.get("filter_version"),s.get("output_cache_key"))
        if not sm: raise HTTPException(409, "result manifest invalid")
        ref=next((r for r in sm.get("input_artifacts") or [] if r.get("type")=="result"), None)
        if not ref: raise HTTPException(404, "full report unavailable")
        amp=self._safe_manifest_path(ref.get("manifest_path"))
        if file_sha(amp) != ref.get("manifest_hash"):
            raise HTTPException(409, "source result manifest hash mismatch")
        astep = next((step for step in run.get("steps", []) if step.get("output_manifest_path") and self._safe_manifest_path(step.get("output_manifest_path")) == amp), None)
        if astep and astep.get("status") not in (StepStatus.succeeded, StepStatus.succeeded_cached):
            raise HTTPException(409, {"status":"not_ready"})
        am=self.artifacts.read_manifest_metadata(amp,"result",astep.get("filter") if astep else None,astep.get("filter_version") if astep else None,astep.get("output_cache_key") if astep else None)
        if not am: raise HTTPException(409, "source result manifest invalid")
        entry=am["files"][0]; path=resolve_artifact_file(amp, entry)
        meta={k:v for k,v in entry.items() if k!="sha256"}
        meta.update({"manifest_path":str(amp),"filename":path.name})
        return {"path":path,"entry":entry,"metadata":meta}

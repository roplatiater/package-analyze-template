import json
from pathlib import Path
from fastapi import HTTPException
from app.pipeline.models import StepStatus
from app.repositories.artifact_store import ArtifactStore
from app.repositories.run_store import RunStore

class ResultService:
    def __init__(self): self.runs=RunStore()
    def list(self, limit: int = 20):
        items = []
        artifacts = ArtifactStore()
        for p in self.runs.list_runs():
            try:
                run = self.runs.get_run(p["run_id"]); s = run["steps"][-1]
                if s["status"] not in (StepStatus.succeeded, StepStatus.succeeded_cached):
                    continue
                mp = Path(s["output_manifest_path"])
                m = artifacts.valid_manifest(mp,s["output_artifact_type"],s["filter"],s["filter_version"],s.get("output_cache_key"))
                if not m: continue
                data = json.loads((mp.parent / m["files"][0]["path"]).read_text())
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
        m=ArtifactStore().valid_manifest(mp,s["output_artifact_type"],s["filter"],s["filter_version"],s.get("output_cache_key"))
        if not m: raise HTTPException(409, "result manifest invalid")
        try: return json.loads((mp.parent / m["files"][0]["path"]).read_text())
        except Exception as e: raise HTTPException(409, f"result file invalid: {e}")

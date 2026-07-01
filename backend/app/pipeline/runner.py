import asyncio
from pathlib import Path
from app.filters.fetch_raw import FetchRawFilter
from app.filters.analyze import AnalyzeFilter
from app.pipeline.models import PipelineStatus, StepStatus, now_iso
from app.repositories.artifact_store import ArtifactStore
from app.repositories.run_store import RunStore
from datetime import datetime, timezone, timedelta

class PipelineRunner:
    def __init__(self): self.runs=RunStore(); self.artifacts=ArtifactStore(); self.fetch=FetchRawFilter(); self.analyze=AnalyzeFilter()
    async def run(self, run_id: str):
        for _ in range(5):
            p=self.runs.get_pipeline(run_id); p["status"]=PipelineStatus.running; p["updated_at"]=now_iso(); self.runs.save_pipeline(run_id,p)
            fetch_status=await self._fetch(run_id)
            if fetch_status in (StepStatus.succeeded, StepStatus.succeeded_cached): await self._analyze(run_id)
            p=self.runs.get_pipeline(run_id); steps=[self.runs.get_step(run_id,"fetch_raw"), self.runs.get_step(run_id,"analyze")]
            statuses={s["status"] for s in steps}
            if steps[1]["status"] in (StepStatus.succeeded, StepStatus.succeeded_cached): p["status"]=PipelineStatus.succeeded
            elif StepStatus.failed in statuses: p["status"]=PipelineStatus.failed
            elif StepStatus.blocked in statuses: p["status"]=PipelineStatus.blocked
            else: p["status"]=PipelineStatus.running
            p["updated_at"]=now_iso(); self.runs.save_pipeline(run_id,p)
            retrying=[s for s in steps if s["status"]==StepStatus.retrying]
            if not retrying: return
            due=min(datetime.fromisoformat(s["next_retry_at"]) for s in retrying if s.get("next_retry_at"))
            await asyncio.sleep(max(0,(due-datetime.now(timezone.utc)).total_seconds()))
            for s in retrying:
                s["status"]=StepStatus.ready; self.runs.save_step(run_id,s["step_id"],s)
    async def _fetch(self, run_id):
        p=self.runs.get_pipeline(run_id); req=p["request"]; params=req.get("params",{})
        s=self.runs.get_step(run_id,"fetch_raw")
        if s["status"] in (StepStatus.succeeded,StepStatus.succeeded_cached): return s["status"]
        fparams = params | ({"refresh_nonce": run_id} if params.get("force_refresh") else {})
        key=self.fetch.build_cache_key(req["source"],req["dataset"],fparams); target=self.artifacts.raw_dir(req["source"],req["dataset"],key); s["output_cache_key"]=key
        lock=self.artifacts.lock_for(key)
        async with lock:
            m=self.artifacts.valid_manifest(target/"manifest.json","raw","FetchRawFilter",self.fetch.version,key) if params.get("simulate_fail_stage") != "fetch" else None
            if m: s.update(status=StepStatus.succeeded_cached, output_manifest_path=str(target/"manifest.json"), finished_at=now_iso(), error=None); self.runs.save_step(run_id,"fetch_raw",s); return StepStatus.succeeded_cached
            return await self._execute_fetch_locked(run_id, req, params, s, key, target)

    async def _execute_fetch_locked(self, run_id, req, params, s, key, target):
        try:
            s.update(status=StepStatus.running, attempt=s["attempt"]+1, started_at=now_iso(), heartbeat_at=now_iso(), error=None); self.runs.save_step(run_id,"fetch_raw",s)
            tmp=self.artifacts.tmp_dir(f"{run_id}_fetch_raw_{s['attempt']}"); fp,sha=await self.fetch.run(req["source"],req["dataset"],params,tmp)
            manifest={"schema_version":"1","artifact_type":"raw","cache_key":key,"producer":{"filter":"FetchRawFilter","version":self.fetch.version},"input":{"source":req["source"],"dataset":req["dataset"],"params":params},"files":[{"path":fp.name,"sha256":sha}]}
            if params.get("simulate_fail_stage")=="commit": raise RuntimeError("simulated commit failure")
            mp=self.artifacts.commit(tmp,target,manifest,"raw","FetchRawFilter",self.fetch.version); s.update(status=StepStatus.succeeded, output_manifest_path=mp, finished_at=now_iso())
            self.runs.save_step(run_id,"fetch_raw",s); return StepStatus.succeeded
        except Exception as e:
            st=StepStatus.retrying if s["attempt"]<s["max_attempts"] else StepStatus.failed; nr=(datetime.now(timezone.utc)+timedelta(seconds=1)).isoformat() if st==StepStatus.retrying else None
            s.update(status=st,error=str(e),next_retry_at=nr,finished_at=now_iso() if st==StepStatus.failed else None); self.runs.save_step(run_id,"fetch_raw",s); return st
    async def _analyze(self, run_id):
        p=self.runs.get_pipeline(run_id); req=p["request"]; params=req.get("params",{})
        raw=self.runs.get_step(run_id,"fetch_raw"); s=self.runs.get_step(run_id,"analyze")
        if raw["status"] not in (StepStatus.succeeded,StepStatus.succeeded_cached): s.update(status=StepStatus.blocked,error="raw not ready"); self.runs.save_step(run_id,"analyze",s); return StepStatus.blocked
        raw_manifest_path = raw.get("output_manifest_path")
        raw_cache_key = raw.get("output_cache_key")
        if not raw_manifest_path or not raw_cache_key or not self.artifacts.valid_manifest(Path(raw_manifest_path), "raw", "FetchRawFilter", self.fetch.version, raw_cache_key):
            s.update(status=StepStatus.blocked,error="MISSING_OR_INVALID_RAW_MANIFEST",finished_at=now_iso())
            self.runs.save_step(run_id,"analyze",s)
            return StepStatus.blocked
        raw_hash=self.artifacts.manifest_hash(raw_manifest_path); aparams = params | ({"refresh_nonce": run_id} if params.get("force_refresh") else {})
        key=self.analyze.build_cache_key(req["analysis_type"],aparams,raw_hash); target=self.artifacts.result_dir(req["analysis_type"],key); s["output_cache_key"]=key; s["input"]={"raw_manifest_path":raw["output_manifest_path"]}
        lock=self.artifacts.lock_for(key)
        async with lock:
            m=self.artifacts.valid_manifest(target/"manifest.json","result","AnalyzeFilter",self.analyze.version,key,raw_hash) if params.get("simulate_fail_stage") != "analyze" else None
            if m: s.update(status=StepStatus.succeeded_cached, output_manifest_path=str(target/"manifest.json"), finished_at=now_iso(), error=None); self.runs.save_step(run_id,"analyze",s); return StepStatus.succeeded_cached
            return await self._execute_analyze_locked(run_id, req, params, raw, raw_hash, s, key, target)

    async def _execute_analyze_locked(self, run_id, req, params, raw, raw_hash, s, key, target):
        try:
            s.update(status=StepStatus.running, attempt=s["attempt"]+1, started_at=now_iso(), heartbeat_at=now_iso(), error=None); self.runs.save_step(run_id,"analyze",s)
            tmp=self.artifacts.tmp_dir(f"{run_id}_analyze_{s['attempt']}"); fp,sha=await self.analyze.run(raw["output_manifest_path"],req["analysis_type"],params,tmp)
            manifest={"schema_version":"1","artifact_type":"result","cache_key":key,"producer":{"filter":"AnalyzeFilter","version":self.analyze.version},"input":{"analysis_type":req["analysis_type"],"params":params},"input_artifacts":[{"type":"raw","manifest_path":raw["output_manifest_path"],"manifest_hash":raw_hash}],"files":[{"path":fp.name,"sha256":sha}]}
            mp=self.artifacts.commit(tmp,target,manifest,"result","AnalyzeFilter",self.analyze.version); s.update(status=StepStatus.succeeded, output_manifest_path=mp, finished_at=now_iso()); self.runs.save_step(run_id,"analyze",s); return StepStatus.succeeded
        except Exception as e:
            st=StepStatus.retrying if s["attempt"]<s["max_attempts"] else StepStatus.failed; nr=(datetime.now(timezone.utc)+timedelta(seconds=1)).isoformat() if st==StepStatus.retrying else None
            s.update(status=st,error=str(e),next_retry_at=nr,finished_at=now_iso() if st==StepStatus.failed else None); self.runs.save_step(run_id,"analyze",s); return st

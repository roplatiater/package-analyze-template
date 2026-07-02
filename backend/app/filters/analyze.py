import asyncio, json
from collections import Counter
from pathlib import Path
from app.filters.base import BaseFilter, FilterOutput
from app.repositories.artifact_store import stable_hash, json_manifest_entry, write_json_artifact, ArtifactStore

class AnalyzeFilter(BaseFilter):
    name="AnalyzeFilter"; version="1.0.0"
    def build_cache_key(self, context):
        params=context.get("params",{}); raw_manifest_hash=context["input_artifacts"][0]["manifest_hash"]
        clean={k:v for k,v in params.items() if k not in {"simulate_fail_stage","delay_ms","force_refresh"}}
        return stable_hash({"analysis_type":context["run_request"]["analysis_type"],"params":clean,"version":self.version,"raw_manifest_hash":raw_manifest_hash})
    async def run(self, context, tmp_dir):
        params=context.get("params",{}); raw_manifest_path=context["input_artifacts"][0]["manifest_path"]
        if params.get("delay_ms"): await asyncio.sleep(params["delay_ms"]/1000)
        if params.get("simulate_fail_stage") == "analyze": raise RuntimeError("simulated analyze failure")
        store = ArtifactStore(); mp = Path(raw_manifest_path)
        raw_manifest=store.valid_manifest(mp, "raw", "UnzipPackageFilter", "1.0.0") or store.valid_manifest(mp, "raw", "FetchRawFilter", "1.0.0")
        if not raw_manifest: raise RuntimeError("invalid raw manifest")
        raw_data=json.loads((Path(raw_manifest_path).parent/raw_manifest["files"][0]["path"]).read_text())
        records=raw_data["records"]
        phrase_counts=Counter(r["phrase"] for r in records); category_counts=Counter(r["category"] for r in records)
        result={"analysis_type":context["run_request"]["analysis_type"],"summary":{"record_count":len(records),"average_value":sum(r["value"] for r in records)/len(records),"average_phrase_length":sum(len(r["phrase"]) for r in records)/len(records)},"phrase_counts":dict(phrase_counts),"category_counts":dict(category_counts),"chart_data":[{"label":k,"value":v} for k,v in phrase_counts.items()],"records_preview":records[:5]}
        fp=write_json_artifact(tmp_dir,"result.json",result)
        return FilterOutput([fp])
    def build_manifest(self, context, output_files):
        return {"schema_version":"1","artifact_type":"result","cache_key":context["cache_key"],"producer":{"filter":self.name,"version":self.version},"input":{"analysis_type":context["run_request"]["analysis_type"],"params":context.get("params",{})},"input_artifacts":context["input_artifacts"],"files":[json_manifest_entry(p, "result.json") for p in output_files.files]}

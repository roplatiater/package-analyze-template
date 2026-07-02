from pathlib import Path
from app.filters.base import BaseFilter, FilterOutput
from app.repositories.artifact_store import stable_hash, json_manifest_entry, read_json_artifact, write_json_artifact, ArtifactStore

class SummarizeResultFilter(BaseFilter):
    name="SummarizeResultFilter"; version="1.0.0"
    def build_cache_key(self, context):
        h=context["input_artifacts"][0]["manifest_hash"]
        return stable_hash({"version":self.version,"result_manifest_hash":h})
    async def run(self, context, tmp_dir):
        mp=Path(context["input_artifacts"][0]["manifest_path"])
        m=ArtifactStore().valid_manifest(mp,"result","AnalyzeFilter","1.0.0")
        if not m: raise RuntimeError("invalid result manifest")
        data=read_json_artifact(mp, m["files"][0])
        out={"summary":data.get("summary",{}),"chart_data":data.get("chart_data",[]),"records_preview":data.get("records_preview",[]),"source_result_manifest":str(mp)}
        fp=write_json_artifact(tmp_dir,"summary.json",out, compression="none")
        return FilterOutput([fp])
    def build_manifest(self, context, output_files):
        return {"schema_version":"1","artifact_type":"summary","cache_key":context["cache_key"],"producer":{"filter":self.name,"version":self.version},"input_artifacts":context["input_artifacts"],"files":[json_manifest_entry(p, "summary.json") for p in output_files.files]}

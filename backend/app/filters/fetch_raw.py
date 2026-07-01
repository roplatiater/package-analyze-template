import asyncio, json, zipfile
from app.filters.base import BaseFilter, FilterOutput
from app.repositories.artifact_store import stable_hash, file_sha

class FetchRawFilter(BaseFilter):
    name="FetchRawFilter"; version="1.1.0"
    def build_cache_key(self, context):
        req=context["run_request"]; params=context.get("params",{})
        clean={k:v for k,v in params.items() if k not in {"simulate_fail_stage","delay_ms","force_refresh"}}
        return stable_hash({"source":req["source"],"dataset":req["dataset"],"params":clean,"version":self.version})
    async def run(self, context, tmp_dir):
        req=context["run_request"]; params=context.get("params",{})
        if params.get("delay_ms"): await asyncio.sleep(params["delay_ms"]/1000)
        if params.get("simulate_fail_stage") == "fetch": raise RuntimeError("simulated fetch failure")
        phrases=params.get("phrases") or ["alpha pipeline","beta cache","gamma filter","alpha pipeline","delta manifest","beta cache"]
        categories=params.get("categories") or ["demo","cache","filter"]
        mult=params.get("value_multiplier") if params.get("value_multiplier") is not None else 1
        records=params.get("records") or [{"id":i+1,"phrase":p,"category":categories[i%len(categories)],"value":(i+1)*7*mult,"event_time":"2026-07-01T00:%02d:00Z"%i} for i,p in enumerate(phrases)]
        data={"source":req["source"],"dataset":req["dataset"],"records":records}
        fp=tmp_dir/"package.zip"
        with zipfile.ZipFile(fp, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.json", json.dumps(data, indent=2))
            zf.writestr("package_meta.json", json.dumps({"source":req["source"],"dataset":req["dataset"],"format":"demo-package-v1"}, indent=2))
        return FilterOutput([fp])
    def build_manifest(self, context, output_files):
        req=context["run_request"]
        return {"schema_version":"1","artifact_type":"raw_package","cache_key":context["cache_key"],"producer":{"filter":self.name,"version":self.version},"input":{"source":req["source"],"dataset":req["dataset"],"params":context.get("params",{})},"files":[{"path":p.name,"sha256":file_sha(p)} for p in output_files.files]}

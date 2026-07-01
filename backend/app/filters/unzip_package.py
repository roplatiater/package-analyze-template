import json, zipfile
from pathlib import Path, PurePosixPath

from app.core.config import settings
from app.filters.base import BaseFilter, FilterOutput
from app.repositories.artifact_store import ArtifactStore, file_sha, stable_hash


class UnzipPackageFilter(BaseFilter):
    name = "UnzipPackageFilter"; version = "1.0.0"

    def build_cache_key(self, context):
        raw_package_manifest_hash = context["input_artifacts"][0]["manifest_hash"]
        return stable_hash({"version": self.version, "raw_package_manifest_hash": raw_package_manifest_hash})

    async def run(self, context, tmp_dir):
        ref = context["input_artifacts"][0]
        manifest_path = Path(ref["manifest_path"])
        manifest = ArtifactStore().valid_manifest(manifest_path, "raw_package", "FetchRawFilter", "1.1.0")
        if not manifest:
            raise RuntimeError("invalid raw_package manifest")
        files = manifest.get("files") or []
        if len(files) != 1 or files[0].get("path") != "package.zip":
            raise RuntimeError("raw_package must contain exactly package.zip")
        zip_path = manifest_path.parent / "package.zip"
        max_members = settings.zip_max_members
        max_file = settings.zip_max_file_uncompressed_bytes
        max_total = settings.zip_max_total_uncompressed_bytes
        seen = set(); data_entries = [] ; total = 0; members = []
        with zipfile.ZipFile(zip_path) as zf:
            infos = zf.infolist()
            if len(infos) > max_members:
                raise RuntimeError("zip has too many members")
            for info in infos:
                name = info.filename
                parts = PurePosixPath(name).parts
                if name.startswith("/") or "\\" in name or (len(name) >= 2 and name[1] == ":") or ".." in parts:
                    raise RuntimeError("unsafe zip member path")
                if name in seen:
                    raise RuntimeError("duplicate zip member")
                seen.add(name)
                if info.flag_bits & 0x1:
                    raise RuntimeError("encrypted zip member rejected")
                if info.file_size > max_file:
                    raise RuntimeError("zip member too large")
                total += info.file_size
                if total > max_total:
                    raise RuntimeError("zip total too large")
                if name == "data.json":
                    if info.is_dir():
                        raise RuntimeError("data.json cannot be a directory")
                    data_entries.append(info)
                members.append({"name": name, "compressed_size": info.compress_size, "uncompressed_size": info.file_size, "sha256": None})
            if len(data_entries) != 1:
                raise RuntimeError("zip must contain one root data.json")
            raw_bytes = zf.read(data_entries[0])
        data = json.loads(raw_bytes.decode("utf-8"))
        if not isinstance(data, dict) or not isinstance(data.get("records"), list) or not all(isinstance(r, dict) for r in data["records"]):
            raise RuntimeError("data.json must be object with records: list[dict]")
        req = context["run_request"]
        normalized = {"source": data.get("source", req["source"]), "dataset": data.get("dataset", req["dataset"]), "records": data["records"]}
        data_fp = tmp_dir / "data.json"
        data_fp.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
        with zipfile.ZipFile(zip_path) as zf:
            for m in members:
                if not m["name"].endswith("/"):
                    import hashlib
                    h = hashlib.sha256(zf.read(m["name"])).hexdigest()
                    m["sha256"] = h
        meta_fp = tmp_dir / "extracted_manifest.json"
        meta_fp.write_text(json.dumps({"members": members, "total_uncompressed_size": total}, indent=2), encoding="utf-8")
        return FilterOutput([data_fp, meta_fp])

    def build_manifest(self, context, output_files):
        return {"schema_version":"1","artifact_type":"raw","cache_key":context["cache_key"],"producer":{"filter":self.name,"version":self.version},"input":{},"input_artifacts":context["input_artifacts"],"files":[{"path":p.name,"sha256":file_sha(p)} for p in output_files.files]}

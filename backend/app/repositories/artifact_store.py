import asyncio, hashlib, json, os, shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from app.core.config import settings


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:24]


def file_sha(path: Path) -> str:
    h = hashlib.sha256(); h.update(path.read_bytes()); return h.hexdigest()


class ArtifactStore:
    _locks: dict[str, asyncio.Lock] = {}
    def __init__(self):
        self.root = settings.data_root; (self.root/"tmp").mkdir(parents=True, exist_ok=True); (self.root/"locks").mkdir(exist_ok=True)

    def raw_dir(self, source, dataset, key): return self.root/"artifacts"/"raw"/source/dataset/key
    def result_dir(self, typ, key): return self.root/"artifacts"/"results"/typ/key
    def tmp_dir(self, step_run_id):
        p = self.root/"tmp"/step_run_id; shutil.rmtree(p, ignore_errors=True); p.mkdir(parents=True); return p

    def manifest_hash(self, path: str) -> str: return file_sha(Path(path))

    def lock_for(self, cache_key: str) -> asyncio.Lock:
        self._locks.setdefault(cache_key, asyncio.Lock())
        return self._locks[cache_key]

    def valid_manifest(self, path: Path, artifact_type: str, producer: str, version: str, expected_cache_key: str | None = None, expected_input_manifest_hash: str | None = None) -> dict[str, Any] | None:
        try:
            if not path.exists(): return None
            m = json.loads(path.read_text())
            if m.get("schema_version") != "1" or m.get("artifact_type") != artifact_type or m.get("producer", {}).get("filter") != producer or m.get("producer", {}).get("version") != version: return None
            if expected_cache_key and m.get("cache_key") != expected_cache_key: return None
            files = m.get("files")
            if not isinstance(files, list) or not files: return None
            if expected_input_manifest_hash is not None:
                arts = m.get("input_artifacts") or []
                if not arts or arts[0].get("manifest_hash") != expected_input_manifest_hash: return None
            base = path.parent
            for f in files:
                rel = Path(f.get("path", ""))
                if rel.is_absolute() or ".." in rel.parts: return None
                fp = base / rel
                if not fp.exists() or file_sha(fp) != f["sha256"]: return None
            return m
        except Exception:
            return None

    def commit(self, tmp: Path, target: Path, manifest: dict[str, Any], artifact_type: str, producer: str, version: str) -> str:
        existing = self.valid_manifest(target/"manifest.json", artifact_type, producer, version, manifest.get("cache_key"))
        if existing:
            shutil.rmtree(tmp, ignore_errors=True); return str(target/"manifest.json")
        if target.exists(): shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)
        for item in tmp.iterdir(): shutil.copy2(item, target / item.name)
        manifest["created_at"] = datetime.now(timezone.utc).isoformat(); manifest["expires_at"] = (datetime.now(timezone.utc)+timedelta(days=30)).isoformat()
        mp = target / "manifest.json"; t = mp.with_suffix(".json.tmp")
        t.write_text(json.dumps(manifest, indent=2), encoding="utf-8"); os.replace(t, mp)
        shutil.rmtree(tmp, ignore_errors=True); return str(mp)

    def clean_tmp(self):
        tmp = self.root / "tmp"
        if tmp.exists():
            for p in tmp.iterdir():
                if p.is_dir(): shutil.rmtree(p, ignore_errors=True)

    def cache_status(self):
        raw = list((self.root/"artifacts"/"raw").glob("*/*/*/manifest.json")) if (self.root/"artifacts"/"raw").exists() else []
        raw_pkg = list((self.root/"artifacts"/"raw_packages").glob("*/*/*/manifest.json")) if (self.root/"artifacts"/"raw_packages").exists() else []
        res = list((self.root/"artifacts"/"results").glob("*/*/manifest.json")) if (self.root/"artifacts"/"results").exists() else []
        return {"raw_count": len(raw), "raw_package_count": len(raw_pkg), "result_count": len(res)}

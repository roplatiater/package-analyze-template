import asyncio, gzip, hashlib, json, os, shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from app.core.config import settings


_JSON_ARTIFACT_ORIGINAL_SIZES: dict[str, int] = {}


class _CountingTextWriter:
    def __init__(self, wrapped):
        self.wrapped = wrapped
        self.bytes_written = 0
    def write(self, s: str):
        self.bytes_written += len(s.encode("utf-8"))
        return self.wrapped.write(s)
    def flush(self):
        return self.wrapped.flush()


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:24]


def file_sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def artifact_compression() -> str:
    value = getattr(settings, "artifact_compression", "none").lower()
    return value if value in {"none", "gzip"} else "none"


def json_artifact_name(logical_base: str, compression: str | None = None) -> str:
    comp = artifact_compression() if compression is None else compression
    return f"{logical_base}.gz" if comp == "gzip" else logical_base


def write_json_artifact(directory: Path, logical_base: str, data: Any, compression: str | None = None) -> Path:
    comp = artifact_compression() if compression is None else compression
    comp = comp if comp in {"none", "gzip"} else "none"
    path = directory / json_artifact_name(logical_base, comp)
    if comp == "gzip":
        with gzip.open(path, "wt", encoding="utf-8", compresslevel=int(getattr(settings, "artifact_compression_level", 1))) as f:
            counter = _CountingTextWriter(f)
            json.dump(data, counter, indent=2)
            counter.write("\n")
            _JSON_ARTIFACT_ORIGINAL_SIZES[str(path.resolve())] = counter.bytes_written
    else:
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
    return path


def resolve_artifact_file(manifest_path: Path, entry: dict[str, Any]) -> Path:
    rel = Path(entry.get("path", ""))
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError("unsafe artifact path")
    path = (manifest_path.parent / rel).resolve()
    path.relative_to(manifest_path.parent.resolve())
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def open_artifact_file(manifest_path: Path, entry: dict[str, Any]):
    path = resolve_artifact_file(manifest_path, entry)
    compression = entry.get("compression") or ("gzip" if path.name.endswith(".gz") else "none")
    return gzip.open(path, "rt", encoding="utf-8") if compression == "gzip" else path.open("rt", encoding="utf-8")


def read_json_artifact(manifest_path: Path, entry: dict[str, Any] | None = None) -> Any:
    selected: dict[str, Any] = entry or json.loads(manifest_path.read_text())["files"][0]
    with open_artifact_file(manifest_path, selected) as f:
        return json.load(f)


def json_manifest_entry(path: Path, logical_base: str | None = None, content_type: str = "application/json") -> dict[str, Any]:
    compression = "gzip" if path.name.endswith(".gz") else "none"
    entry: dict[str, Any] = {"path": path.name, "sha256": file_sha(path), "content_type": content_type, "compression": compression}
    size = path.stat().st_size
    if compression == "gzip":
        original = _JSON_ARTIFACT_ORIGINAL_SIZES.get(str(path.resolve()))
        if original is None:
            # Fallback for pre-existing gzip artifacts. Gzip ISIZE is modulo 2^32,
            # so writers should prefer write_json_artifact() for exact metadata.
            with path.open("rb") as f:
                f.seek(-4, os.SEEK_END)
                original = int.from_bytes(f.read(4), "little")
        entry.update({"logical_path": logical_base or path.name.removesuffix(".gz"), "original_size_bytes": original, "compressed_size_bytes": size, "size_bytes": size})
    else:
        entry.update({"original_size_bytes": size, "size_bytes": size})
    return entry


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

    def read_manifest_metadata(self, path: Path, artifact_type: str | None = None, producer: str | None = None, version: str | None = None, expected_cache_key: str | None = None) -> dict[str, Any] | None:
        try:
            root = self.root.resolve(); rp = path.resolve(); rp.relative_to(root)
            m = json.loads(path.read_text())
            if m.get("schema_version") != "1": return None
            if artifact_type and m.get("artifact_type") != artifact_type: return None
            if producer and m.get("producer", {}).get("filter") != producer: return None
            if version and m.get("producer", {}).get("version") != version: return None
            if expected_cache_key and m.get("cache_key") != expected_cache_key: return None
            files = m.get("files")
            if not isinstance(files, list) or not files: return None
            for f in files:
                resolve_artifact_file(path, f)
            for ref in m.get("input_artifacts") or []:
                ref_path = ref.get("manifest_path")
                if ref_path: Path(ref_path).resolve().relative_to(root)
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

import json, os, uuid
from pathlib import Path
from typing import Any
from app.core.config import settings
from app.pipeline.models import pipeline_doc
from app.pipeline.templates import build_steps, template_metadata
from fastapi import HTTPException


class JsonRunStore:
    def __init__(self):
        self.root = settings.data_root / "runs"
        self.index = settings.data_root / "index" / "runs.json"
        self.root.mkdir(parents=True, exist_ok=True); self.index.parent.mkdir(parents=True, exist_ok=True)

    def _write_json(self, path: Path, data: dict[str, Any]):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        os.replace(tmp, path)

    def _read_json(self, path: Path) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    def create(self, request: dict[str, Any]) -> str:
        run_id = uuid.uuid4().hex[:12]
        base = self.root / run_id / "steps"; base.mkdir(parents=True, exist_ok=True)
        meta=template_metadata()
        self._write_json(self.root / run_id / "pipeline.json", pipeline_doc(run_id, request, meta["step_order"], meta["final_step_id"]))
        for s in build_steps(): self._write_json(base / f"{s['step_id']}.json", s)
        ids = self.list_ids(); ids.insert(0, run_id); self._write_json(self.index, {"run_ids": ids[:100]})
        return run_id

    def list_ids(self):
        ids = []
        if self.index.exists():
            try: ids = self._read_json(self.index).get("run_ids", [])
            except Exception: ids = []
        scanned = [p.parent.name for p in self.root.glob("*/pipeline.json")]
        return list(dict.fromkeys(ids + scanned))

    def exists(self, run_id): return (self.root / run_id / "pipeline.json").exists()
    def get_pipeline(self, run_id):
        if not self.exists(run_id): raise HTTPException(404, "run not found")
        return self._read_json(self.root / run_id / "pipeline.json")
    def save_pipeline(self, run_id, data): self._write_json(self.root / run_id / "pipeline.json", data)
    def get_step(self, run_id, step): return self._read_json(self.root / run_id / "steps" / f"{step}.json")
    def save_step(self, run_id, step, data): self._write_json(self.root / run_id / "steps" / f"{step}.json", data)
    def get_run(self, run_id):
        steps=[self._read_json(p) for p in sorted((self.root/run_id/"steps").glob("*.json"))]
        p=self.get_pipeline(run_id); order={sid:i for i,sid in enumerate(p.get("step_order", []))}
        return {"pipeline": p, "steps": sorted(steps, key=lambda s: order.get(s.get("step_id"), 999))}
    def list_runs(self): return [self.get_run(i)["pipeline"] for i in self.list_ids() if (self.root/i/"pipeline.json").exists()]


class RunStore:
    def __new__(cls):
        if settings.metadata_store == "sqlite":
            from app.repositories.sqlite_store import SQLiteStore
            store = SQLiteStore()
            try: store.import_json_store(JsonRunStore())
            except Exception: pass
            return store
        return JsonRunStore()

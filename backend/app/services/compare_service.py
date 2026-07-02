import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.core.config import settings
from app.pipeline.models import StepStatus
from app.repositories.artifact_store import ArtifactStore, read_json_artifact, stable_hash
from app.repositories.run_store import RunStore


RUN_ID_RE = re.compile(r"^[a-f0-9]{12}$")
PREVIEW_LIMIT = 100


class CompareService:
    def __init__(self):
        self.runs = RunStore()
        self.artifacts = ArtifactStore()
        self.data_root = settings.data_root.resolve()

    def compare(self, left_run_id: str | None, right_run_id: str | None) -> dict[str, Any]:
        left = self._load_run_artifacts(left_run_id)
        right = self._load_run_artifacts(right_run_id)
        phrase_delta = self._count_delta(left["analysis"].get("phrase_counts", {}), right["analysis"].get("phrase_counts", {}))
        category_delta = self._count_delta(left["analysis"].get("category_counts", {}), right["analysis"].get("category_counts", {}))

        return {
            "left": {"run_id": left_run_id, "label": self._label(left), "summary": left["summary"].get("summary", {})},
            "right": {"run_id": right_run_id, "label": self._label(right), "summary": right["summary"].get("summary", {})},
            "summary_delta": self._summary_delta(left["summary"].get("summary", {}), right["summary"].get("summary", {})),
            "phrase_delta": phrase_delta[:PREVIEW_LIMIT],
            "category_delta": category_delta[:PREVIEW_LIMIT],
            "record_diff": self._record_diff(left["raw"].get("records", []), right["raw"].get("records", [])),
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "compare_key": stable_hash({"left_run_id": left_run_id, "right_run_id": right_run_id}),
                "delta_semantics": "right_minus_left",
                "preview_limit": PREVIEW_LIMIT,
                "phrase_delta_total": len(phrase_delta),
                "phrase_delta_truncated": len(phrase_delta) > PREVIEW_LIMIT,
                "category_delta_total": len(category_delta),
                "category_delta_truncated": len(category_delta) > PREVIEW_LIMIT,
            },
        }

    def _load_run_artifacts(self, run_id: str | None) -> dict[str, Any]:
        if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
            raise HTTPException(404, "run not found")
        try:
            run = self.runs.get_run(run_id)
        except Exception:
            raise HTTPException(404, "run not found")

        pipeline = run.get("pipeline", {})
        final_step_id = pipeline.get("final_step_id")
        final_step = next((s for s in run.get("steps", []) if s.get("step_id") == final_step_id), None)
        if not final_step or final_step.get("status") not in (StepStatus.succeeded, StepStatus.succeeded_cached):
            raise HTTPException(409, {"status": "not_ready"})
        if not final_step.get("output_manifest_path"):
            raise HTTPException(409, "result manifest missing")
        if final_step.get("output_artifact_type") != "summary":
            raise HTTPException(409, "final step type mismatch")
        if not final_step.get("filter") or not final_step.get("filter_version") or not final_step.get("output_cache_key"):
            raise HTTPException(409, "final step metadata missing")

        summary_path = self._safe_manifest_path(final_step["output_manifest_path"])
        expected_summary_input_hash = self._first_input_hash(final_step)
        summary_manifest = self._valid_manifest(
            summary_path,
            final_step.get("output_artifact_type"),
            final_step.get("filter"),
            final_step.get("filter_version"),
            final_step.get("output_cache_key"),
            expected_input_manifest_hash=expected_summary_input_hash,
        )
        summary_data = self._read_artifact_json(summary_path, summary_manifest)
        self._validate_summary_content(summary_data)

        analysis_ref = self._single_input(summary_manifest, "result")
        analysis_path = self._safe_manifest_path(analysis_ref.get("manifest_path"))
        self._verify_manifest_hash(analysis_path, analysis_ref.get("manifest_hash"))
        analysis_step = self._find_step(run, analysis_path, "result")
        analysis_manifest = self._valid_manifest(analysis_path, "result", analysis_step.get("filter"), analysis_step.get("filter_version"), analysis_step.get("output_cache_key"), analysis_ref.get("manifest_hash"))
        analysis_data = self._read_artifact_json(analysis_path, analysis_manifest)
        self._validate_analysis_content(analysis_data)

        raw_ref = self._single_input(analysis_manifest, "raw")
        raw_path = self._safe_manifest_path(raw_ref.get("manifest_path"))
        self._verify_manifest_hash(raw_path, raw_ref.get("manifest_hash"))
        raw_step = self._find_step(run, raw_path, "raw")
        raw_manifest = self._valid_manifest(raw_path, "raw", raw_step.get("filter"), raw_step.get("filter_version"), raw_step.get("output_cache_key"), raw_ref.get("manifest_hash"))
        raw_data = self._read_artifact_json(raw_path, raw_manifest)
        self._validate_raw_content(raw_data)

        return {"pipeline": pipeline, "final_step": final_step, "summary": summary_data, "analysis": analysis_data, "raw": raw_data}

    def _safe_manifest_path(self, value: str | None) -> Path:
        if not value:
            raise HTTPException(409, "manifest path missing")
        try:
            path = Path(value).resolve()
            path.relative_to(self.data_root)
            return path
        except Exception:
            raise HTTPException(409, "manifest path invalid")

    def _valid_manifest(self, path: Path, artifact_type: str | None, producer: str | None, version: str | None, cache_key: str | None = None, manifest_hash: str | None = None, expected_input_manifest_hash: str | None = None) -> dict[str, Any]:
        if not artifact_type or not producer or not version:
            raise HTTPException(409, "manifest validation metadata missing")
        if manifest_hash is not None:
            self._verify_manifest_hash(path, manifest_hash)
        manifest = self.artifacts.valid_manifest(path, artifact_type, producer, version, cache_key, expected_input_manifest_hash)
        if not manifest:
            raise HTTPException(409, "manifest invalid")
        return manifest

    def _verify_manifest_hash(self, path: Path, expected: str | None):
        try:
            actual = self.artifacts.manifest_hash(str(path))
        except Exception:
            raise HTTPException(409, "manifest hash mismatch")
        if not expected or actual != expected:
            raise HTTPException(409, "manifest hash mismatch")

    def _first_input_hash(self, step: dict[str, Any]) -> str | None:
        inputs = step.get("input_artifacts") or []
        if not inputs:
            return None
        return inputs[0].get("manifest_hash")

    def _single_input(self, manifest: dict[str, Any], artifact_type: str) -> dict[str, Any]:
        for item in manifest.get("input_artifacts") or []:
            if item.get("type") == artifact_type:
                return item
        raise HTTPException(409, "artifact provenance invalid")

    def _find_step(self, run: dict[str, Any], manifest_path: Path, artifact_type: str) -> dict[str, Any]:
        for step in run.get("steps", []):
            step_manifest = step.get("output_manifest_path")
            if not step_manifest:
                continue
            try:
                step_path = Path(step_manifest).resolve()
                step_path.relative_to(self.data_root)
            except Exception:
                continue
            if step_path == manifest_path:
                if step.get("status") not in (StepStatus.succeeded, StepStatus.succeeded_cached):
                    raise HTTPException(409, "artifact step not ready")
                if step.get("output_artifact_type") != artifact_type:
                    raise HTTPException(409, "artifact step type mismatch")
                if not step.get("filter") or not step.get("filter_version") or not step.get("output_cache_key"):
                    raise HTTPException(409, "artifact step metadata missing")
                return step
        raise HTTPException(409, "artifact provenance step missing")

    def _read_artifact_json(self, manifest_path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
        try:
            return read_json_artifact(manifest_path, manifest["files"][0])
        except Exception as e:
            raise HTTPException(409, f"artifact file invalid: {e}")

    def _validate_summary_content(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict) or not isinstance(data.get("summary"), dict):
            raise HTTPException(409, "artifact content invalid")
        if "chart_data" in data and not isinstance(data.get("chart_data"), list):
            raise HTTPException(409, "artifact content invalid")
        if "records_preview" in data and not isinstance(data.get("records_preview"), list):
            raise HTTPException(409, "artifact content invalid")

    def _validate_analysis_content(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict) or not isinstance(data.get("summary"), dict):
            raise HTTPException(409, "artifact content invalid")
        for key in ("phrase_counts", "category_counts"):
            counts = data.get(key)
            if not isinstance(counts, dict) or not all(isinstance(k, str) and isinstance(v, (int, float)) for k, v in counts.items()):
                raise HTTPException(409, "artifact content invalid")

    def _validate_raw_content(self, data: dict[str, Any]) -> None:
        records = data.get("records") if isinstance(data, dict) else None
        if not isinstance(records, list) or not all(isinstance(r, dict) for r in records):
            raise HTTPException(409, "artifact content invalid")
        for record in records:
            if "id" in record and record.get("id") is not None and not isinstance(record.get("id"), (str, int, float, bool)):
                raise HTTPException(409, "artifact content invalid")

    def _label(self, side: dict[str, Any]) -> str:
        req = side.get("pipeline", {}).get("request", {})
        return req.get("params", {}).get("batch_id") or side.get("pipeline", {}).get("run_id", "")

    def _summary_delta(self, left: dict[str, Any], right: dict[str, Any]) -> dict[str, float]:
        keys = set(left) | set(right)
        return {k: right.get(k, 0) - left.get(k, 0) for k in sorted(keys) if isinstance(left.get(k, 0), (int, float)) and isinstance(right.get(k, 0), (int, float))}

    def _count_delta(self, left: dict[str, Any], right: dict[str, Any]) -> list[dict[str, Any]]:
        rows = []
        for key in sorted(set(left) | set(right)):
            l, r = left.get(key, 0), right.get(key, 0)
            rows.append({"label": key, "left": l, "right": r, "delta": r - l})
        return sorted(rows, key=lambda row: (-abs(row["delta"]), row["label"]))

    def _record_map(self, records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        explicit = [r.get("id") for r in records if r.get("id") is not None]
        dupes = [k for k, v in Counter(explicit).items() if v > 1]
        if dupes:
            raise HTTPException(409, {"error": "duplicate record ids", "ids": dupes[:PREVIEW_LIMIT]})
        counts = defaultdict(int)
        out = {}
        for record in records:
            if record.get("id") is not None:
                key = f"id:{record['id']}"
            else:
                h = stable_hash(record); counts[h] += 1; key = f"hash:{h}:{counts[h]}"
            out[key] = record
        return out

    def _record_diff(self, left_records: list[dict[str, Any]], right_records: list[dict[str, Any]]) -> dict[str, Any]:
        left = self._record_map(left_records); right = self._record_map(right_records)
        added = [right[k] for k in sorted(set(right) - set(left))]
        removed = [left[k] for k in sorted(set(left) - set(right))]
        changed = [{"key": k, "left": left[k], "right": right[k]} for k in sorted(set(left) & set(right)) if left[k] != right[k]]
        return {
            "added_count": len(added), "removed_count": len(removed), "changed_count": len(changed),
            "added_preview": added[:PREVIEW_LIMIT], "removed_preview": removed[:PREVIEW_LIMIT], "changed_preview": changed[:PREVIEW_LIMIT],
        }

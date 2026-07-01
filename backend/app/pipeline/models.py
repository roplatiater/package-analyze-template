from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PipelineStatus(StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    blocked = "blocked"


class StepStatus(StrEnum):
    pending = "pending"
    ready = "ready"
    running = "running"
    retrying = "retrying"
    succeeded = "succeeded"
    succeeded_cached = "succeeded_cached"
    failed = "failed"
    blocked = "blocked"


def pipeline_doc(run_id: str, request: dict[str, Any], step_order: list[str] | None = None, final_step_id: str | None = None) -> dict[str, Any]:
    return {"run_id": run_id, "status": PipelineStatus.queued, "request": request, "step_order": step_order or [], "final_step_id": final_step_id, "created_at": now_iso(), "updated_at": now_iso()}


def step_doc(step_id: str, step_type: str, filter_name: str, version: str, max_attempts: int, status: StepStatus, depends_on=None, output_artifact_type=None) -> dict[str, Any]:
    return {
        "step_id": step_id, "step_type": step_type, "depends_on": depends_on or [],
        "filter": filter_name, "filter_version": version, "status": status,
        "attempt": 0, "max_attempts": max_attempts, "input": {}, "input_artifacts": [],
        "output_artifact_type": output_artifact_type, "output_cache_key": None,
        "expected_output_manifest_path": None, "output_manifest_path": None,
        "worker_id": None, "claimed_at": None, "lease_until": None,
        "started_at": None, "heartbeat_at": None, "finished_at": None,
        "error": None, "next_retry_at": None,
    }

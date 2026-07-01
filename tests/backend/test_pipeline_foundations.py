import asyncio
import json
import zipfile
from pathlib import Path

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.api.models import CreateJobRequest
from app.core.config import settings
from app.filters.analyze import AnalyzeFilter
from app.filters.unzip_package import UnzipPackageFilter
from app.pipeline.models import PipelineStatus, StepStatus
from app.pipeline.notification_robot import check_once
from app.pipeline.step_worker import StepWorker
from app.repositories.artifact_store import ArtifactStore, file_sha, stable_hash
from app.repositories.run_store import RunStore
from app.services.compare_service import CompareService
from app.services.job_service import JobService


async def run_chain(run_id):
    for step_type in ["fetch_raw", "unzip_package", "analyze_phrase_stats", "summarize_result"]:
        assert await StepWorker(step_type).run_once()
    return RunStore().get_run(run_id)


def test_create_job_and_full_worker_chain_result_exists():
    run = JobService().create({"params": {"batch_id": "t1"}})
    final = asyncio.run(run_chain(run["pipeline"]["run_id"]))
    assert final["pipeline"]["status"] == PipelineStatus.succeeded
    last = final["steps"][-1]
    assert last["status"] in (StepStatus.succeeded, StepStatus.succeeded_cached)
    assert last["output_manifest_path"]


def test_unzip_rejects_path_traversal_zip(tmp_path):
    z = tmp_path / "package.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("../evil.txt", "x")
        zf.writestr("data.json", '{"records": []}')
    m = {"schema_version":"1","artifact_type":"raw_package","cache_key":"k","producer":{"filter":"FetchRawFilter","version":"1.1.0"},"files":[{"path":"package.zip","sha256":file_sha(z)}]}
    (tmp_path / "manifest.json").write_text(json.dumps(m))
    ctx = {"input_artifacts": [{"manifest_path": str(tmp_path / "manifest.json"), "manifest_hash": file_sha(tmp_path / "manifest.json")}], "run_request": {"source":"s", "dataset":"d"}}
    with pytest.raises(RuntimeError, match="unsafe"):
        asyncio.run(UnzipPackageFilter().run(ctx, tmp_path / "out"))


def test_old_fetch_raw_v1_manifest_accepted_by_analyze(tmp_path):
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"records":[{"phrase":"a","category":"c","value":2}]}))
    m = {"schema_version":"1","artifact_type":"raw","cache_key":"k","producer":{"filter":"FetchRawFilter","version":"1.0.0"},"files":[{"path":"data.json","sha256":file_sha(data)}]}
    mp = tmp_path / "manifest.json"; mp.write_text(json.dumps(m))
    ctx = {"run_request":{"analysis_type":"phrase_stats"},"params":{},"input_artifacts":[{"manifest_path":str(mp),"manifest_hash":file_sha(mp)}],"cache_key":"rk"}
    outdir = tmp_path / "out"; outdir.mkdir()
    out = asyncio.run(AnalyzeFilter().run(ctx, outdir))
    assert json.loads(out.files[0].read_text())["summary"]["record_count"] == 1


def test_compare_success_for_two_completed_runs():
    left = JobService().create({"params": {"batch_id": "left", "phrases": ["a"]}})["pipeline"]["run_id"]
    right = JobService().create({"params": {"batch_id": "right", "phrases": ["a", "b"]}})["pipeline"]["run_id"]
    asyncio.run(run_chain(left)); asyncio.run(run_chain(right))
    result = CompareService().compare(left, right)
    assert result["right"]["run_id"] == right


def test_robot_idempotency_and_unrelated_tuple_ignore(monkeypatch):
    other = JobService().create({"source":"other", "params": {"batch_id": "other"}})["pipeline"]["run_id"]
    a = JobService().create({"params": {"batch_id": "a"}})["pipeline"]["run_id"]
    b = JobService().create({"params": {"batch_id": "b"}})["pipeline"]["run_id"]
    asyncio.run(run_chain(other)); asyncio.run(run_chain(a)); asyncio.run(run_chain(b))
    import app.pipeline.notification_robot as robot
    monkeypatch.setattr(robot, "STATE_PATH", settings.data_root / "index" / "notification_state.json")
    first = asyncio.run(check_once())
    second = asyncio.run(check_once())
    assert first and {first["left_run_id"], first["right_run_id"]} == {a, b}
    assert other not in {first["left_run_id"], first["right_run_id"]}
    assert second is None


def test_cache_status_includes_raw_package_count():
    run = JobService().create({"params": {"batch_id": "cache"}})["pipeline"]["run_id"]
    asyncio.run(run_chain(run))
    status = ArtifactStore().cache_status()
    assert "raw_package_count" in status and status["raw_package_count"] >= 1


def test_invalid_compare_run_id_404():
    with pytest.raises(HTTPException):
        CompareService().compare("not-a-run", "also-bad")


def test_job_params_validate_active_failure_stages_only():
    assert CreateJobRequest.model_validate({"params": {"simulate_fail_stage": "commit"}}).params.simulate_fail_stage == "commit"
    with pytest.raises(ValidationError):
        CreateJobRequest.model_validate({"params": {"simulate_fail_stage": "unzip"}})
    with pytest.raises(ValidationError):
        CreateJobRequest.model_validate({"params": {"simulate_fail_stage": "summarize"}})


def test_value_multiplier_zero_is_honored():
    run = JobService().create({"params": {"batch_id": "zero", "value_multiplier": 0}})["pipeline"]["run_id"]
    final = asyncio.run(run_chain(run))
    assert final["pipeline"]["status"] == PipelineStatus.succeeded
    result_path = final["steps"][-1]["output_manifest_path"]
    manifest_path = Path(result_path)
    manifest = json.loads(manifest_path.read_text())
    data = json.loads((manifest_path.parent / manifest["files"][0]["path"]).read_text())
    assert data["summary"]["average_value"] == 0


def test_force_refresh_changes_fetch_cache_key():
    first = JobService().create({"params": {"batch_id": "refresh"}})["pipeline"]["run_id"]
    second = JobService().create({"params": {"batch_id": "refresh", "force_refresh": True}})["pipeline"]["run_id"]
    asyncio.run(StepWorker("fetch_raw").run_once())
    asyncio.run(StepWorker("fetch_raw").run_once())
    steps1 = {s["step_id"]: s for s in RunStore().get_run(first)["steps"]}
    steps2 = {s["step_id"]: s for s in RunStore().get_run(second)["steps"]}
    assert steps1["fetch_raw"]["output_cache_key"] != steps2["fetch_raw"]["output_cache_key"]
    assert RunStore().get_pipeline(second)["request"]["params"]["refresh_nonce"] == second

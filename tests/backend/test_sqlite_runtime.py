import asyncio
from datetime import datetime, timezone, timedelta

import pytest
from fastapi import HTTPException

from app.core.config import settings
from app.pipeline.models import PipelineStatus, StepStatus
from app.pipeline.step_executor import StepExecutor
from app.pipeline.step_worker import StepWorker
from app.repositories.run_store import JsonRunStore, RunStore
from app.repositories.sqlite_store import SQLiteStore
from app.services.job_service import JobService


@pytest.fixture
def sqlite_mode(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "metadata_store", "sqlite")
    monkeypatch.setattr(settings, "sqlite_path", tmp_path / "metadata.db")
    monkeypatch.setattr(settings, "worker_lease_seconds", 60)
    yield
    monkeypatch.setattr(settings, "metadata_store", "json")


async def run_chain():
    for step_type in ["fetch_raw", "unzip_package", "analyze_phrase_stats", "summarize_result"]:
        assert await StepWorker(step_type).run_once()


def test_sqlite_mode_full_worker_chain(sqlite_mode):
    run = JobService().create({"params": {"batch_id": "sqlite-full"}})
    run_id = run["pipeline"]["run_id"]
    assert isinstance(RunStore(), SQLiteStore)
    asyncio.run(run_chain())
    final = RunStore().get_run(run_id)
    assert final["pipeline"]["status"] == PipelineStatus.succeeded
    assert final["steps"][-1]["output_manifest_path"]


def test_sqlite_downstream_invalid_manifest_not_executed(sqlite_mode):
    run_id = JobService().create({"params": {"batch_id": "bad-dep"}})["pipeline"]["run_id"]
    store = RunStore()
    fetch = store.get_step(run_id, "fetch_raw")
    fetch.update(status=StepStatus.succeeded, output_manifest_path="/missing/manifest.json", output_cache_key="k")
    store.save_step(run_id, "fetch_raw", fetch)
    assert store.promote_ready_steps(run_id) == 1
    assert asyncio.run(StepWorker("unzip_package", worker_id="u1").run_once()) is False
    unzip = store.get_step(run_id, "unzip")
    assert unzip["status"] == StepStatus.blocked
    assert unzip["error"] == "DEPENDENCY_INVALID"
    assert not (settings.data_root / "artifacts" / "raw").exists()


def test_sqlite_stale_executor_finish_does_not_write_metadata(sqlite_mode):
    store = RunStore()
    run_id = store.create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    past = datetime.now(timezone.utc) - timedelta(seconds=120)
    assert store.claim_ready_step("fetch_raw", "stale", now=past) == (run_id, "fetch_raw")
    step = store.get_step(run_id, "fetch_raw")
    req = store.get_pipeline(run_id)["request"]
    from app.filters.registry import registry
    item = registry.get("fetch_raw")
    key = item.filter_class().build_cache_key({"run_id":run_id,"run_request":req,"step":step,"params":{},"input_artifacts":[]})
    assert store.prepare_claimed_step(run_id, "fetch_raw", "stale", 1, {"output_cache_key":key, "expected_output_manifest_path":str(item.output_path_builder(settings.data_root, req, key)/"manifest.json"), "input_artifacts":[]}, now=past)
    asyncio.run(StepExecutor().execute(run_id, "fetch_raw"))
    after = store.get_step(run_id, "fetch_raw")
    assert after["status"] == StepStatus.running
    assert after["output_manifest_path"] is None


def test_sqlite_jobservice_retry_conflicts_with_running(sqlite_mode):
    run_id = JobService().create({"params": {"batch_id": "retry-conflict"}})["pipeline"]["run_id"]
    assert RunStore().claim_ready_step("fetch_raw", "worker") == (run_id, "fetch_raw")
    with pytest.raises(HTTPException) as e:
        JobService().retry(run_id)
    assert e.value.status_code == 409


def test_sqlite_runstore_imports_json_idempotently_on_factory(sqlite_mode, monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "metadata_store", "json")
    json_store = JsonRunStore()
    run_id = json_store.create({"source":"json", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    monkeypatch.setattr(settings, "metadata_store", "sqlite")
    first = RunStore(); second = RunStore()
    assert first.exists(run_id)
    assert second.exists(run_id)
    assert first.list_ids().count(run_id) == 1

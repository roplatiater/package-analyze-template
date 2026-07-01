from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

import pytest
from fastapi import HTTPException

from app.pipeline.models import PipelineStatus, StepStatus
from app.repositories.run_store import RunStore
from app.repositories.sqlite_store import SQLiteStore


def store(tmp_path):
    return SQLiteStore(tmp_path / "metadata.db")


def test_schema_init_and_runstore_compatible_create_get_list(tmp_path):
    s = store(tmp_path)
    run_id = s.create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    assert s.exists(run_id)
    assert s.list_ids() == [run_id]
    run = s.get_run(run_id)
    assert run["pipeline"]["run_id"] == run_id
    assert [x["step_id"] for x in run["steps"]] == ["fetch_raw", "unzip", "analyze", "summarize"]
    assert s.list_runs()[0]["request"]["source"] == "s"


def test_import_json_store_idempotent(tmp_path):
    json_store = RunStore()
    run_id = json_store.create({"source":"json", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    s = store(tmp_path)
    assert s.import_json_store(json_store) == 1
    assert s.import_json_store(json_store) == 0
    assert s.get_pipeline(run_id) == json_store.get_pipeline(run_id)
    assert len(s.get_run(run_id)["steps"]) == 4


def test_import_json_store_does_not_overwrite_existing_sqlite_state(tmp_path):
    json_store = RunStore()
    run_id = json_store.create({"source":"json", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    s = store(tmp_path)
    assert s.import_json_store(json_store) == 1
    p = s.get_pipeline(run_id)
    p["status"] = PipelineStatus.running
    s.save_pipeline(run_id, p)
    assert s.import_json_store(json_store) == 0
    assert s.get_pipeline(run_id)["status"] == PipelineStatus.running


def test_concurrent_double_claim_only_one_succeeds(tmp_path):
    db = tmp_path / "metadata.db"
    run_id = SQLiteStore(db).create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    def claim(worker):
        return SQLiteStore(db).claim_ready_step("fetch_raw", worker)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(claim, ["w1", "w2"]))
    assert results.count((run_id, "fetch_raw")) == 1
    assert results.count(None) == 1


def test_stale_worker_cannot_heartbeat_or_finish_after_recovery(tmp_path):
    s = store(tmp_path)
    run_id = s.create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    past = datetime.now(timezone.utc) - timedelta(seconds=120)
    assert s.claim_ready_step("fetch_raw", "A", now=past) == (run_id, "fetch_raw")
    assert s.recover_expired_leases(datetime.now(timezone.utc)) == 1
    assert not s.heartbeat_step(run_id, "fetch_raw", "A", 1)
    assert not s.finish_step(run_id, "fetch_raw", "A", 1, "/tmp/manifest.json")
    assert s.claim_ready_step("fetch_raw", "B") == (run_id, "fetch_raw")
    assert s.get_step(run_id, "fetch_raw")["worker_id"] == "B"


def test_expired_lease_without_recovery_blocks_owned_writes(tmp_path):
    s = store(tmp_path)
    run_id = s.create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    claimed_at = datetime.now(timezone.utc) - timedelta(seconds=120)
    now = datetime.now(timezone.utc)
    assert s.claim_ready_step("fetch_raw", "A", now=claimed_at) == (run_id, "fetch_raw")
    assert not s.heartbeat_step(run_id, "fetch_raw", "A", 1, now=now)
    assert not s.finish_step(run_id, "fetch_raw", "A", 1, "/tmp/manifest.json", now=now)
    assert not s.fail_or_retry_step(run_id, "fetch_raw", "A", 1, "boom", now=now)
    step = s.get_step(run_id, "fetch_raw")
    assert step["status"] == StepStatus.running
    assert step["worker_id"] == "A"


def test_prepare_claimed_step_owner_attempt_and_lease_guard(tmp_path):
    s = store(tmp_path)
    run_id = s.create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    now = datetime.now(timezone.utc)
    assert s.claim_ready_step("fetch_raw", "A", now=now) == (run_id, "fetch_raw")
    updates = {
        "input_artifacts": [{"type":"raw", "manifest_path":"/tmp/in.json", "manifest_hash":"abc"}],
        "output_cache_key": "cache-key-1",
        "expected_output_manifest_path": "/tmp/out/manifest.json",
        "error": None,
    }
    assert s.prepare_claimed_step(run_id, "fetch_raw", "A", 1, updates, now=now + timedelta(seconds=1))
    step = s.get_step(run_id, "fetch_raw")
    assert step["input_artifacts"] == updates["input_artifacts"]
    assert step["output_cache_key"] == "cache-key-1"
    assert step["expected_output_manifest_path"] == "/tmp/out/manifest.json"

    with s._connect() as con:
        row = con.execute("SELECT output_cache_key FROM steps WHERE run_id=? AND step_id=?", (run_id, "fetch_raw")).fetchone()
    assert row[0] == "cache-key-1"
    assert not s.prepare_claimed_step(run_id, "fetch_raw", "B", 1, {"output_cache_key":"wrong-worker"}, now=now)
    assert not s.prepare_claimed_step(run_id, "fetch_raw", "A", 2, {"output_cache_key":"wrong-attempt"}, now=now)
    assert not s.prepare_claimed_step(run_id, "fetch_raw", "A", 1, {"output_cache_key":"expired"}, now=now + timedelta(seconds=120))
    assert s.get_step(run_id, "fetch_raw")["output_cache_key"] == "cache-key-1"


def test_retry_pipeline_conflicts_with_running_step(tmp_path):
    s = store(tmp_path)
    run_id = s.create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    s.claim_ready_step("fetch_raw", "A")
    with pytest.raises(HTTPException) as e:
        s.retry_pipeline(run_id)
    assert e.value.status_code == 409
    assert s.get_step(run_id, "fetch_raw")["status"] == StepStatus.running


def test_retry_pipeline_removes_simulated_failure_param(tmp_path):
    s = store(tmp_path)
    run_id = s.create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{"simulate_fail_stage":"analyze"}})
    p = s.get_pipeline(run_id)
    p["status"] = PipelineStatus.failed
    s.save_pipeline(run_id, p)
    retried = s.retry_pipeline(run_id)
    assert "simulate_fail_stage" not in retried["pipeline"]["request"]["params"]


def test_promote_ready_steps_after_dependencies_succeeded(tmp_path):
    s = store(tmp_path)
    run_id = s.create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    fetch = s.get_step(run_id, "fetch_raw")
    fetch.update(status=StepStatus.succeeded, output_manifest_path="/tmp/raw_package_manifest.json")
    s.save_step(run_id, "fetch_raw", fetch)
    assert s.promote_ready_steps(run_id) == 1
    assert s.get_step(run_id, "unzip")["status"] == StepStatus.ready


def test_claim_ready_step_dependency_check_is_status_only_foundation(tmp_path):
    s = store(tmp_path)
    run_id = s.create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    fetch = s.get_step(run_id, "fetch_raw")
    fetch.update(status=StepStatus.succeeded, output_manifest_path="/missing/manifest.json")
    s.save_step(run_id, "fetch_raw", fetch)
    assert s.promote_ready_steps(run_id) == 1
    # Phase 2 SQLite claims intentionally trust dependency statuses only.
    # Phase 3 worker runtime must validate dependency manifests before execute.
    assert s.claim_ready_step("unzip_package", "worker") == (run_id, "unzip")


def test_finish_step_updates_pipeline_and_promotes(tmp_path):
    s = store(tmp_path)
    run_id = s.create({"source":"s", "dataset":"d", "analysis_type":"phrase_stats", "trigger":"manual", "params":{}})
    s.claim_ready_step("fetch_raw", "A")
    assert s.finish_step(run_id, "fetch_raw", "A", 1, "/tmp/m.json")
    assert s.get_step(run_id, "fetch_raw")["worker_id"] is None
    assert s.get_step(run_id, "unzip")["status"] == StepStatus.ready
    assert s.get_pipeline(run_id)["status"] == PipelineStatus.running

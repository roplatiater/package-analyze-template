from fastapi.testclient import TestClient

from app.core.config import settings
from app.main import app
from app.services.job_service import JobService


def client():
    return TestClient(app)


def test_ready_and_metrics_json_mode(monkeypatch):
    monkeypatch.setattr(settings, "metadata_store", "json")
    JobService().create({"params": {"batch_id": "metrics-json"}})
    c = client()
    assert c.get("/api/health").status_code == 200
    ready = c.get("/api/health/ready")
    assert ready.status_code == 200
    metrics = c.get("/api/metrics")
    assert metrics.status_code == 200
    body = metrics.json()
    assert body["metadata_store"] == "json"
    assert "jobs_by_status" in body and "steps_by_status" in body


def test_ready_and_metrics_sqlite_mode(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "metadata_store", "sqlite")
    monkeypatch.setattr(settings, "sqlite_path", tmp_path / "metadata.db")
    JobService().create({"params": {"batch_id": "metrics-sqlite"}})
    c = client()
    ready = c.get("/api/health/ready")
    assert ready.status_code == 200
    assert ready.json()["checks"]["sqlite"] == "ok"
    metrics = c.get("/api/metrics").json()
    assert metrics["metadata_store"] == "sqlite"
    assert metrics["jobs_by_status"].get("queued", 0) >= 1


def test_auth_fail_closed_protects_write_and_metrics_but_not_health(monkeypatch):
    monkeypatch.setattr(settings, "auth_enabled", True)
    monkeypatch.setattr(settings, "auth_trusted_header_enabled", False)
    c = client()
    assert c.get("/api/health").status_code == 200
    assert c.get("/api/health/ready").status_code == 200
    assert c.post("/api/jobs", json={}).status_code == 403
    assert c.get("/api/jobs").status_code == 403
    assert c.get("/api/metrics").status_code == 403


def test_auth_trusted_header_dev_mode(monkeypatch):
    monkeypatch.setattr(settings, "auth_enabled", True)
    monkeypatch.setattr(settings, "auth_trusted_header_enabled", True)
    assert client().get("/api/metrics", headers={"x-auth-user": "proxy-user"}).status_code == 200

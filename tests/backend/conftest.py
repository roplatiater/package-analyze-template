import pytest

from app.core.config import settings


@pytest.fixture(autouse=True)
def isolated_settings(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "data_root", tmp_path)
    monkeypatch.setattr(settings, "notification_outbox_dir", tmp_path / "outbox" / "notifications")
    monkeypatch.setattr(settings, "auto_pipeline_enabled", False)
    monkeypatch.setattr(settings, "notification_robot_enabled", False)
    monkeypatch.setattr(settings, "inprocess_workers_enabled", False)
    yield

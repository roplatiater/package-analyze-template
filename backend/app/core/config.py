from pathlib import Path
import os

def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default).lower()).lower() in {"1", "true", "yes", "on"}

def _list(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    return [x.strip() for x in value.split(",") if x.strip()] if value else default


class Settings:
    app_name = "DataFilter Pipeline Demo"
    backend_root = Path(__file__).resolve().parents[2]
    data_root = Path(os.getenv("DATA_ROOT", str(backend_root / "data")))
    metadata_store = os.getenv("METADATA_STORE", "json")
    sqlite_path = Path(os.getenv("SQLITE_PATH", str(data_root / "metadata.db")))
    sqlite_busy_timeout_ms = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "5000"))
    default_source = "local_demo"
    default_dataset = "sample"
    default_analysis_type = "phrase_stats"
    auto_pipeline_enabled = _bool("AUTO_PIPELINE_ENABLED", True)
    inprocess_workers_enabled = _bool("INPROCESS_WORKERS_ENABLED", True)
    auto_pipeline_interval_seconds = int(os.getenv("AUTO_PIPELINE_INTERVAL_SECONDS", "10"))
    notification_robot_enabled = _bool("NOTIFICATION_ROBOT_ENABLED", True)
    notification_robot_interval_seconds = int(os.getenv("NOTIFICATION_ROBOT_INTERVAL_SECONDS", "30"))
    notification_outbox_dir = data_root / "outbox" / "notifications"
    zip_max_members = int(os.getenv("ZIP_MAX_MEMBERS", "20"))
    zip_max_file_uncompressed_bytes = int(os.getenv("ZIP_MAX_FILE_UNCOMPRESSED_BYTES", str(1024 * 1024)))
    zip_max_total_uncompressed_bytes = int(os.getenv("ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES", str(2 * 1024 * 1024)))
    worker_lease_seconds = int(os.getenv("WORKER_LEASE_SECONDS", "60"))
    worker_heartbeat_interval_seconds = int(os.getenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", str(max(1, worker_lease_seconds // 3))))
    cors_origins = _list("CORS_ORIGINS", [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ])
    auth_enabled = _bool("AUTH_ENABLED", False)
    oidc_issuer_url = os.getenv("OIDC_ISSUER_URL", "")
    oidc_audience = os.getenv("OIDC_AUDIENCE", "")
    oidc_jwks_url = os.getenv("OIDC_JWKS_URL", "")
    auth_trusted_header_enabled = _bool("AUTH_TRUSTED_HEADER_ENABLED", False)


settings = Settings()

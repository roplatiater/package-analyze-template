import sqlite3
from fastapi import APIRouter, HTTPException
from app.core.config import settings
router=APIRouter()
@router.get("")
async def health(): return {"status":"ok"}
@router.get("/ready")
async def ready():
    try:
        settings.data_root.mkdir(parents=True, exist_ok=True)
        probe = settings.data_root / ".ready_probe"
        probe.write_text("ok", encoding="utf-8"); probe.unlink(missing_ok=True)
        checks = {"data_root": "ok", "metadata_store": settings.metadata_store}
        if settings.metadata_store == "sqlite":
            from app.repositories.sqlite_store import SQLiteStore
            SQLiteStore()._init_schema()
            con = sqlite3.connect(settings.sqlite_path); con.execute("SELECT 1"); con.close()
            checks["sqlite"] = "ok"
        return {"status":"ready", "checks": checks}
    except Exception as e:
        raise HTTPException(503, {"status":"not_ready", "error": str(e)})

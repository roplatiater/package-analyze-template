import json
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.core.config import settings
from app.pipeline.models import PipelineStatus, StepStatus, now_iso, pipeline_doc
from app.pipeline.templates import build_steps, template_metadata
from app.repositories.run_store import JsonRunStore


OK = {StepStatus.succeeded, StepStatus.succeeded_cached, "succeeded", "succeeded_cached"}


class SQLiteStore:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path or settings.sqlite_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self):
        con = sqlite3.connect(self.path, timeout=max(1, settings.sqlite_busy_timeout_ms) / 1000, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute(f"PRAGMA busy_timeout={int(settings.sqlite_busy_timeout_ms)}")
        con.execute("PRAGMA foreign_keys=ON")
        return con

    def _init_schema(self):
        with self._connect() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs(
                  run_id TEXT PRIMARY KEY,
                  status TEXT, source TEXT, dataset TEXT, analysis_type TEXT, trigger TEXT,
                  created_at TEXT, updated_at TEXT, pipeline_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS steps(
                  run_id TEXT NOT NULL, step_id TEXT NOT NULL, step_type TEXT, status TEXT,
                  depends_on_json TEXT, worker_id TEXT, attempt INTEGER, max_attempts INTEGER,
                  lease_until TEXT, next_retry_at TEXT, output_manifest_path TEXT,
                  output_artifact_type TEXT, filter TEXT, filter_version TEXT, output_cache_key TEXT,
                  step_json TEXT NOT NULL,
                  PRIMARY KEY(run_id, step_id),
                  FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS events(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT, event_type TEXT, run_id TEXT, step_id TEXT, step_type TEXT,
                  status TEXT, duration_ms INTEGER, metadata_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_steps_type_status ON steps(step_type,status);
                CREATE INDEX IF NOT EXISTS idx_steps_status_lease ON steps(status,lease_until);
                CREATE INDEX IF NOT EXISTS idx_steps_status_retry ON steps(status,next_retry_at);
                CREATE INDEX IF NOT EXISTS idx_steps_worker_attempt ON steps(worker_id,attempt);
                CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at);
                CREATE INDEX IF NOT EXISTS idx_runs_tuple ON runs(source,dataset,analysis_type);
                """
            )

    def _run_cols(self, p):
        req = p.get("request", {})
        return (p["run_id"], str(p.get("status")), req.get("source"), req.get("dataset"), req.get("analysis_type"), req.get("trigger"), p.get("created_at"), p.get("updated_at"), json.dumps(p, default=str))

    def _step_cols(self, run_id, s):
        return (run_id, s["step_id"], s.get("step_type"), str(s.get("status")), json.dumps(s.get("depends_on", [])), s.get("worker_id"), s.get("attempt", 0), s.get("max_attempts"), s.get("lease_until"), s.get("next_retry_at"), s.get("output_manifest_path"), s.get("output_artifact_type"), s.get("filter"), s.get("filter_version"), s.get("output_cache_key"), json.dumps(s, default=str))

    def _upsert_pipeline(self, con, p):
        con.execute("""INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(run_id) DO UPDATE SET status=excluded.status, source=excluded.source, dataset=excluded.dataset, analysis_type=excluded.analysis_type, trigger=excluded.trigger, created_at=excluded.created_at, updated_at=excluded.updated_at, pipeline_json=excluded.pipeline_json""", self._run_cols(p))

    def _upsert_step(self, con, run_id, s):
        con.execute("""INSERT INTO steps VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(run_id,step_id) DO UPDATE SET step_type=excluded.step_type,status=excluded.status,depends_on_json=excluded.depends_on_json,worker_id=excluded.worker_id,attempt=excluded.attempt,max_attempts=excluded.max_attempts,lease_until=excluded.lease_until,next_retry_at=excluded.next_retry_at,output_manifest_path=excluded.output_manifest_path,output_artifact_type=excluded.output_artifact_type,filter=excluded.filter,filter_version=excluded.filter_version,output_cache_key=excluded.output_cache_key,step_json=excluded.step_json""", self._step_cols(run_id, s))

    def _record_event_con(self, con, event_type, run_id=None, step_id=None, step_type=None, status=None, duration_ms=None, metadata=None):
        con.execute("INSERT INTO events(created_at,event_type,run_id,step_id,step_type,status,duration_ms,metadata_json) VALUES(?,?,?,?,?,?,?,?)", (now_iso(), event_type, run_id, step_id, step_type, str(status) if status else None, duration_ms, json.dumps(metadata or {}, default=str)))

    def record_event(self, event_type, run_id=None, step_id=None, step_type=None, status=None, duration_ms=None, metadata=None):
        with self._connect() as con:
            self._record_event_con(con, event_type, run_id, step_id, step_type, status, duration_ms, metadata)

    def event_counts(self):
        with self._connect() as con:
            return {r[0]: r[1] for r in con.execute("SELECT event_type, COUNT(*) FROM events GROUP BY event_type")}

    def create(self, request: dict[str, Any]) -> str:
        run_id = uuid.uuid4().hex[:12]
        meta = template_metadata(); p = pipeline_doc(run_id, request, meta["step_order"], meta["final_step_id"])
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            self._upsert_pipeline(con, p)
            for s in build_steps(): self._upsert_step(con, run_id, s)
            con.commit()
        return run_id

    def list_ids(self):
        with self._connect() as con:
            return [r[0] for r in con.execute("SELECT run_id FROM runs ORDER BY created_at DESC")]

    def exists(self, run_id):
        with self._connect() as con: return con.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone() is not None
    def get_pipeline(self, run_id):
        with self._connect() as con:
            r = con.execute("SELECT pipeline_json FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if not r: raise HTTPException(404, "run not found")
        return json.loads(r[0])
    def save_pipeline(self, run_id, data):
        with self._connect() as con: self._upsert_pipeline(con, data)
    def get_step(self, run_id, step_id):
        with self._connect() as con: r = con.execute("SELECT step_json FROM steps WHERE run_id=? AND step_id=?", (run_id, step_id)).fetchone()
        if not r: raise HTTPException(404, "step not found")
        return json.loads(r[0])
    def save_step(self, run_id, step_id, data):
        with self._connect() as con: self._upsert_step(con, run_id, data)
    def get_run(self, run_id):
        p = self.get_pipeline(run_id)
        with self._connect() as con: rows = con.execute("SELECT step_json FROM steps WHERE run_id=?", (run_id,)).fetchall()
        order = {sid: i for i, sid in enumerate(p.get("step_order", []))}
        steps = sorted([json.loads(r[0]) for r in rows], key=lambda s: order.get(s.get("step_id"), 999))
        return {"pipeline": p, "steps": steps}
    def list_runs(self): return [self.get_pipeline(i) for i in self.list_ids()]

    def import_json_store(self, json_run_store=None) -> int:
        src = json_run_store or JsonRunStore(); count = 0
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            for run_id in src.list_ids():
                try: run = src.get_run(run_id)
                except Exception: continue
                if con.execute("SELECT 1 FROM runs WHERE run_id=?", (run_id,)).fetchone() is not None:
                    continue
                count += 1
                self._upsert_pipeline(con, run["pipeline"])
                for s in run["steps"]: self._upsert_step(con, run_id, s)
            con.commit()
        return count

    def _now(self, now=None): return (now or datetime.now(timezone.utc)).isoformat() if not isinstance(now, str) else now
    def _lease(self, now_iso_value): return (datetime.fromisoformat(now_iso_value) + timedelta(seconds=settings.worker_lease_seconds)).isoformat()

    def _deps_ok(self, con, run_id, step) -> bool:
        """Phase 2 queue foundation dependency check.

        This intentionally checks persisted dependency statuses only. Phase 3
        worker integration must perform manifest-aware validation before using
        SQLite claims to execute downstream steps.
        """
        for dep in json.loads(step["depends_on_json"] or "[]"):
            r = con.execute("SELECT status FROM steps WHERE run_id=? AND step_id=?", (run_id, dep)).fetchone()
            if not r or r[0] not in OK: return False
        return True

    def _owned_running_step(self, con, run_id, step_id, worker_id, attempt, now_value):
        return con.execute(
            """SELECT step_json FROM steps
            WHERE run_id=? AND step_id=? AND status=? AND worker_id=? AND attempt=?
              AND lease_until IS NOT NULL AND lease_until>=?""",
            (run_id, step_id, StepStatus.running, worker_id, attempt, now_value),
        ).fetchone()

    def claim_ready_step(self, step_type, worker_id, now=None):
        n = self._now(now)
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            rows = con.execute("SELECT * FROM steps WHERE step_type=? AND (status=? OR (status=? AND (next_retry_at IS NULL OR next_retry_at<=?))) ORDER BY rowid", (step_type, StepStatus.ready, StepStatus.retrying, n)).fetchall()
            for r in rows:
                if not self._deps_ok(con, r["run_id"], r): continue
                s = json.loads(r["step_json"]); attempt = int(s.get("attempt", 0)) + 1
                s.update(status=StepStatus.running, worker_id=worker_id, claimed_at=n, heartbeat_at=n, lease_until=self._lease(n), attempt=attempt, error=None)
                self._upsert_step(con, r["run_id"], s)
                self._record_event_con(con, "step_claimed", r["run_id"], r["step_id"], r["step_type"], StepStatus.running, metadata={"worker_id": worker_id, "attempt": attempt})
                p = json.loads(con.execute("SELECT pipeline_json FROM runs WHERE run_id=?", (r["run_id"],)).fetchone()[0]); p.update(status=PipelineStatus.running, updated_at=n); self._upsert_pipeline(con, p)
                con.commit(); return (r["run_id"], r["step_id"])
            con.commit(); return None

    def heartbeat_step(self, run_id, step_id, worker_id, attempt, now=None) -> bool:
        n = self._now(now); lease = self._lease(n)
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            r = self._owned_running_step(con, run_id, step_id, worker_id, attempt, n)
            if not r: con.commit(); return False
            s = json.loads(r[0]); s.update(heartbeat_at=n, lease_until=lease); self._upsert_step(con, run_id, s); con.commit(); return True

    def prepare_claimed_step(self, run_id, step_id, worker_id, attempt, updates: dict, now=None) -> bool:
        n = self._now(now)
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            r = self._owned_running_step(con, run_id, step_id, worker_id, attempt, n)
            if not r: con.commit(); return False
            s = json.loads(r[0]); s.update(updates); self._upsert_step(con, run_id, s)
            if "status" in updates:
                self._refresh_pipeline(con, run_id, n)
            con.commit(); return True

    def finish_step(self, run_id, step_id, worker_id, attempt, output_manifest_path, status="succeeded", now=None) -> bool:
        n = self._now(now)
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            r = self._owned_running_step(con, run_id, step_id, worker_id, attempt, n)
            if not r: con.commit(); return False
            s = json.loads(r[0]); s.update(status=status, output_manifest_path=output_manifest_path, finished_at=n, error=None, worker_id=None, lease_until=None); self._upsert_step(con, run_id, s)
            self._record_event_con(con, "step_finished", run_id, step_id, s.get("step_type"), status, metadata={"attempt": attempt})
            self.promote_ready_steps(run_id, _con=con); self._refresh_pipeline(con, run_id, n); con.commit(); return True

    def fail_or_retry_step(self, run_id, step_id, worker_id, attempt, error, next_retry_at=None, now=None) -> bool:
        n = self._now(now); st = StepStatus.retrying if next_retry_at else StepStatus.failed
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            r = self._owned_running_step(con, run_id, step_id, worker_id, attempt, n)
            if not r: con.commit(); return False
            s = json.loads(r[0]); s.update(status=st, error=error, next_retry_at=next_retry_at, finished_at=n if st == StepStatus.failed else None, worker_id=None, lease_until=None); self._upsert_step(con, run_id, s)
            self._record_event_con(con, "step_failed_or_retrying", run_id, step_id, s.get("step_type"), st, metadata={"attempt": attempt, "error": error})
            self._refresh_pipeline(con, run_id, n); con.commit(); return True

    def recover_expired_leases(self, now=None) -> int:
        n = self._now(now); count = 0; touched_runs = set()
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            for r in con.execute("SELECT run_id,step_json FROM steps WHERE status=? AND lease_until IS NOT NULL AND lease_until<?", (StepStatus.running, n)).fetchall():
                s = json.loads(r["step_json"])
                status = StepStatus.retrying if s.get("attempt", 0) < s.get("max_attempts", 1) else StepStatus.failed
                s.update(status=status, error="LEASE_EXPIRED", worker_id=None, lease_until=None, heartbeat_at=None, next_retry_at=n if status == StepStatus.retrying else None)
                self._upsert_step(con, r["run_id"], s); count += 1; touched_runs.add(r["run_id"])
                self._record_event_con(con, "lease_recovered", r["run_id"], s.get("step_id"), s.get("step_type"), status)
            for run_id in touched_runs:
                self._refresh_pipeline(con, run_id, n)
            con.commit(); return count

    def promote_ready_steps(self, run_id, _con=None) -> int:
        con = _con or self._connect(); close = _con is None; count = 0
        try:
            if close: con.execute("BEGIN IMMEDIATE")
            for r in con.execute("SELECT * FROM steps WHERE run_id=? AND status IN (?,?)", (run_id, StepStatus.pending, StepStatus.blocked)).fetchall():
                if self._deps_ok(con, run_id, r):
                    s = json.loads(r["step_json"]); s.update(status=StepStatus.ready, error=None, worker_id=None, lease_until=None, heartbeat_at=None); self._upsert_step(con, run_id, s); count += 1
            if close: con.commit()
            return count
        finally:
            if close: con.close()

    def retry_pipeline(self, run_id) -> dict:
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            if con.execute("SELECT 1 FROM steps WHERE run_id=? AND status=?", (run_id, StepStatus.running)).fetchone():
                con.commit(); raise HTTPException(409, "cannot retry pipeline with running steps")
            row = con.execute("SELECT pipeline_json FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if not row:
                con.commit(); raise HTTPException(404, "run not found")
            p = json.loads(row[0])
            if p.get("status") not in (PipelineStatus.failed, PipelineStatus.blocked, "failed", "blocked"):
                con.commit(); raise HTTPException(409, "only failed/blocked jobs can be retried")
            p.setdefault("request", {}).setdefault("params", {}).pop("simulate_fail_stage", None); p.update(status=PipelineStatus.queued, updated_at=now_iso()); self._upsert_pipeline(con, p)
            for r in con.execute("SELECT step_json FROM steps WHERE run_id=?", (run_id,)).fetchall():
                s = json.loads(r[0])
                if s.get("status") not in OK:
                    s.update(status=StepStatus.ready if not s.get("depends_on") else StepStatus.pending, attempt=0, error=None, next_retry_at=None, worker_id=None, lease_until=None)
                    self._upsert_step(con, run_id, s)
            self.promote_ready_steps(run_id, _con=con); con.commit()
        return self.get_run(run_id)

    def _refresh_pipeline(self, con, run_id, now_value):
        p = json.loads(con.execute("SELECT pipeline_json FROM runs WHERE run_id=?", (run_id,)).fetchone()[0])
        rows = con.execute("SELECT step_id,status FROM steps WHERE run_id=?", (run_id,)).fetchall(); statuses = {r["status"] for r in rows}
        final = p.get("final_step_id"); final_status = next((r["status"] for r in rows if r["step_id"] == final), None)
        p["status"] = PipelineStatus.succeeded if final_status in OK else PipelineStatus.failed if StepStatus.failed in statuses or "failed" in statuses else PipelineStatus.blocked if StepStatus.blocked in statuses or "blocked" in statuses else PipelineStatus.running
        p["updated_at"] = now_value; self._upsert_pipeline(con, p)

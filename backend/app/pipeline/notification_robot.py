import asyncio, json, os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.pipeline.models import StepStatus
from app.repositories.artifact_store import ArtifactStore
from app.repositories.lock_store import named_lock
from app.repositories.run_store import RunStore
from app.services.compare_service import CompareService


STATE_PATH = settings.data_root / "index" / "notification_state.json"


def _now(): return datetime.now(timezone.utc).isoformat()


def _write_json_atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _load_state() -> dict[str, Any]:
    try: return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception: return {"sent": []}


def _compatible_completed_results():
    runs = RunStore(); artifacts = ArtifactStore(); out = []
    for pipeline in runs.list_runs():
        run_id = pipeline.get("run_id")
        try:
            req = pipeline.get("request", {})
            if (req.get("source"), req.get("dataset"), req.get("analysis_type")) != (settings.default_source, settings.default_dataset, settings.default_analysis_type):
                continue
            full = runs.get_run(run_id)
            final_step_id = full.get("pipeline", {}).get("final_step_id")
            final = next((s for s in full.get("steps", []) if s.get("step_id") == final_step_id), None)
            if not final:
                continue
            if final.get("status") not in (StepStatus.succeeded, StepStatus.succeeded_cached):
                continue
            if not final.get("output_manifest_path"):
                continue
            manifest = artifacts.valid_manifest(Path(final["output_manifest_path"]), final.get("output_artifact_type"), final.get("filter"), final.get("filter_version"), final.get("output_cache_key"))
            if not manifest:
                continue
            out.append({
                "run_id": run_id,
                "trigger": req.get("trigger", "manual"),
                "batch_id": req.get("params", {}).get("batch_id"),
                "created_at": pipeline.get("created_at"),
                "updated_at": pipeline.get("updated_at"),
                "finished_at": final.get("finished_at"),
                "status": pipeline.get("status"),
            })
        except Exception:
            continue
    out.sort(key=lambda x: x.get("finished_at") or x.get("updated_at") or "")
    return out


def _message(compare):
    sd = compare.get("summary_delta", {})
    rd = compare.get("record_diff", {})
    bits = [f"数据更新：{compare['left']['run_id']} → {compare['right']['run_id']}。"]
    if sd:
        bits.append("摘要变化：" + "，".join(f"{k}{v:+g}" for k, v in sd.items()))
    bits.append(f"记录：新增{rd.get('added_count',0)}，删除{rd.get('removed_count',0)}，变更{rd.get('changed_count',0)}。")
    return "".join(bits)


async def check_once():
    results = _compatible_completed_results()
    if len(results) < 2:
        return None
    previous, newest = results[-2], results[-1]
    prev_id, new_id = previous["run_id"], newest["run_id"]
    pair_key = f"{prev_id}:{new_id}"
    outbox = settings.notification_outbox_dir / f"{prev_id}__{new_id}.json"
    with named_lock("notification_robot"):
        state = _load_state(); sent = state.get("sent", [])
        if outbox.exists() or any(s.get("pair_key") == pair_key for s in sent):
            return None
        compare = CompareService().compare(prev_id, new_id)
        payload = {"channel":"outbox","left_run_id":prev_id,"right_run_id":new_id,"pair_key":pair_key,"idempotency_key":pair_key,"message":_message(compare),"compare":compare,"created_at":_now()}
        _write_json_atomic(outbox, payload)
        sent.append({"pair_key": pair_key, "outbox_path": str(outbox), "created_at": payload["created_at"]})
        state["last_notified_pair_key"] = pair_key
        state["last_notified_at"] = payload["created_at"]
        state["sent"] = sent[-50:]
        _write_json_atomic(STATE_PATH, state)
        return payload


async def run_notification_robot():
    while True:
        try:
            await check_once()
        except Exception:
            pass
        await asyncio.sleep(settings.notification_robot_interval_seconds)

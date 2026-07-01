import fcntl
from contextlib import contextmanager
from app.core.config import settings

@contextmanager
def step_lock(run_id: str, step_id: str):
    d=settings.data_root/"locks"; d.mkdir(parents=True, exist_ok=True)
    with open(d/f"step_{run_id}_{step_id}.lock", "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try: yield
        finally: fcntl.flock(f, fcntl.LOCK_UN)

@contextmanager
def artifact_lock(cache_key: str):
    d=settings.data_root/"locks"; d.mkdir(parents=True, exist_ok=True)
    with open(d/f"artifact_{cache_key}.lock", "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try: yield
        finally: fcntl.flock(f, fcntl.LOCK_UN)

@contextmanager
def named_lock(name: str):
    d=settings.data_root/"locks"; d.mkdir(parents=True, exist_ok=True)
    safe = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)
    with open(d/f"{safe}.lock", "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try: yield
        finally: fcntl.flock(f, fcntl.LOCK_UN)

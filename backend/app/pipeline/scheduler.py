import asyncio
from datetime import datetime, timezone
from app.pipeline.models import StepStatus
from app.pipeline.runner import PipelineRunner

class Scheduler:
    def __init__(self):
        self.runner = PipelineRunner(); self.tasks: dict[str, asyncio.Task] = {}
    def enqueue(self, run_id: str):
        if run_id not in self.tasks or self.tasks[run_id].done():
            self.tasks[run_id] = asyncio.create_task(self._run_and_reschedule(run_id))

    async def _run_and_reschedule(self, run_id: str):
        await self.runner.run(run_id)
        run = self.runner.runs.get_run(run_id)
        retry_times = [s.get("next_retry_at") for s in run["steps"] if s.get("status") == StepStatus.retrying and s.get("next_retry_at")]
        if retry_times:
            due = min(datetime.fromisoformat(t) for t in retry_times)
            delay = max(0, (due - datetime.now(timezone.utc)).total_seconds())
            await asyncio.sleep(delay)
            for s in run["steps"]:
                if s.get("status") == StepStatus.retrying:
                    s["status"] = StepStatus.ready; self.runner.runs.save_step(run_id, s["step_id"], s)
            await self._run_and_reschedule(run_id)

scheduler = Scheduler()

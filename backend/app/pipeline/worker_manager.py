import asyncio
from app.filters.registry import registry
from app.pipeline.step_worker import StepWorker

class WorkerManager:
    def __init__(self): self.tasks=[]
    def start(self):
        for st in registry.step_types(): self.tasks.append(asyncio.create_task(StepWorker(st).run_forever()))
    async def stop(self):
        for t in self.tasks: t.cancel()
        for t in self.tasks:
            try: await t
            except asyncio.CancelledError: pass

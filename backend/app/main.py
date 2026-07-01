from contextlib import asynccontextmanager
import asyncio, logging, time, uuid

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.cache import router as cache_router
from app.api.routes.compare import router as compare_router
from app.api.routes.health import router as health_router
from app.api.routes.jobs import router as jobs_router
from app.api.routes.metrics import router as metrics_router
from app.api.routes.results import router as results_router
from app.core.config import settings
from app.pipeline.recovery import recover
from app.pipeline.periodic import run_periodic_trigger
from app.pipeline.notification_robot import run_notification_robot
from app.pipeline.worker_manager import WorkerManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    await recover(clean_tmp=True)
    manager = WorkerManager() if settings.inprocess_workers_enabled else None
    if manager: manager.start()
    periodic_task = asyncio.create_task(run_periodic_trigger()) if settings.auto_pipeline_enabled else None
    notification_task = asyncio.create_task(run_notification_robot()) if settings.notification_robot_enabled else None
    try:
        yield
    finally:
        if periodic_task:
            periodic_task.cancel()
            try:
                await periodic_task
            except asyncio.CancelledError:
                pass
        if notification_task:
            notification_task.cancel()
            try:
                await notification_task
            except asyncio.CancelledError:
                pass
        if manager:
            await manager.stop()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
logging.basicConfig(level=logging.INFO, format="%(message)s")

@app.middleware("http")
async def request_logging(request, call_next):
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    start = time.perf_counter(); status = 500
    try:
        response = await call_next(request); status = response.status_code
        response.headers["x-request-id"] = request_id
        return response
    finally:
        logging.getLogger("app.request").info({"request_id":request_id,"method":request.method,"path":request.url.path,"status":status,"duration_ms":round((time.perf_counter()-start)*1000,2)})

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(health_router, prefix="/api/health", tags=["health"])
app.include_router(jobs_router, prefix="/api/jobs", tags=["jobs"])
app.include_router(results_router, prefix="/api/results", tags=["results"])
app.include_router(cache_router, prefix="/api/cache", tags=["cache"])
app.include_router(compare_router, prefix="/api/compare", tags=["compare"])
app.include_router(metrics_router, prefix="/api/metrics", tags=["metrics"])

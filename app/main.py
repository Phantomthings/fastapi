import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Callable

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from app import routers
from app.cache import init_cache
from app.dependencies import verify_token
from app.logging_config import configure_logging


logger = logging.getLogger("app.request")


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    await init_cache()
    yield


app = FastAPI(title="Charging Analytics API", lifespan=lifespan)
REQUEST_ID_HEADER = "X-Request-ID"


@app.middleware("http")
async def log_requests(request: Request, call_next: Callable):
    request_id = request.headers.get(REQUEST_ID_HEADER, str(uuid.uuid4()))
    start_time = time.perf_counter()

    try:
        response = await call_next(request)
    except Exception:  # pragma: no cover - pass-through to raise after logging
        logger.exception(
            "request_failed",
            extra={
                "event": "request_failed",
                "method": request.method,
                "path": request.url.path,
                "request_id": request_id,
            },
        )
        raise

    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

    logger.info(
        "request_completed",
        extra={
            "event": "request_completed",
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "request_id": request_id,
            "client": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent"),
        },
    )

    response.headers[REQUEST_ID_HEADER] = request_id
    response.headers["X-Process-Time"] = str(duration_ms)
    return response
app.include_router(routers.kpis.router)
app.include_router(routers.sessions.router)
app.include_router(routers.evi.router)


@app.get("/health")
async def healthcheck():
    return JSONResponse(content={"status": "ok"})


@app.get("/secure-check", dependencies=[Depends(verify_token)])
async def secure_check():
    return {"status": "authorized"}

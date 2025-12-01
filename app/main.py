from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse

from app import routers
from app.cache import init_cache
from app.dependencies import verify_token


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_cache()
    yield


app = FastAPI(title="Charging Analytics API", lifespan=lifespan)
app.include_router(routers.kpis.router)
app.include_router(routers.sessions.router)
app.include_router(routers.evi.router)


@app.get("/health")
async def healthcheck():
    return JSONResponse(content={"status": "ok"})


@app.get("/secure-check", dependencies=[Depends(verify_token)])
async def secure_check():
    return {"status": "authorized"}

from fastapi import Request
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
from redis.asyncio import from_url

from app.settings import get_settings


async def init_cache() -> None:
    settings = get_settings()
    redis = from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")


def cache_key_builder(func, namespace: str, request: Request, response=None):  # type: ignore[override]
    params = request.query_params.multi_items()
    parts = [namespace, request.url.path]
    for key, value in sorted(params):
        parts.append(f"{key}={value}")
    return ":".join(parts)


def cache_response(expire: int, namespace: str):
    return cache(expire=expire, namespace=namespace, key_builder=cache_key_builder)

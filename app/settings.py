from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="allow", populate_by_name=True
    )

    database_url: str = Field(alias="DATABASE_URL")
    redis_url: str = Field(alias="REDIS_URL")
    api_token: str = Field(alias="API_TOKEN")
    cache_ttl_kpis: int = Field(default=300, alias="CACHE_TTL_KPIS")
    cache_ttl_kpi_daily: int = Field(default=900, alias="CACHE_TTL_KPI_DAILY")
    cache_ttl_kpi_weekly: int = Field(default=1800, alias="CACHE_TTL_KPI_WEEKLY")
    cache_ttl_sessions: int = Field(default=300, alias="CACHE_TTL_SESSIONS")
    cache_ttl_evi: int = Field(default=300, alias="CACHE_TTL_EVI")
    pool_size: int = Field(default=5, alias="DB_POOL_SIZE")
    max_overflow: int = Field(default=10, alias="DB_MAX_OVERFLOW")
    pool_timeout: int = Field(default=30, alias="DB_POOL_TIMEOUT")
    kpi_view_refresh_minutes: int = Field(default=60, alias="KPI_VIEW_REFRESH_MINUTES")


@lru_cache()
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]

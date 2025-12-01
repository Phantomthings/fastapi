from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool

from app.settings import get_settings

settings = get_settings()
engine = create_async_engine(
    settings.database_url,
    pool_size=settings.pool_size,
    max_overflow=settings.max_overflow,
    pool_timeout=settings.pool_timeout,
    pool_pre_ping=True,
    poolclass=None if settings.pool_size > 0 else NullPool,
)
SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False, autocommit=False)


async def get_db() -> AsyncSession:
    async with SessionLocal() as session:
        yield session

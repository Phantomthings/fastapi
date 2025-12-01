from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import cache_response
from app.database import get_db
from app.dependencies import verify_token
from app.schemas import EviResponse, PaginatedResponse
from app.settings import get_settings

router = APIRouter(prefix="/evi", tags=["evi"], dependencies=[Depends(verify_token)])
settings = get_settings()


async def _fetch_evi(
    db: AsyncSession,
    site_id: Optional[int],
    start_date: Optional[date],
    end_date: Optional[date],
    page: int,
    page_size: int,
) -> PaginatedResponse[EviResponse]:
    filters: List[str] = []
    params: dict = {}

    if site_id is not None:
        filters.append("site_id = :site_id")
        params["site_id"] = site_id
    if start_date is not None:
        filters.append("DATE(occurred_at) >= :start_date")
        params["start_date"] = start_date
    if end_date is not None:
        filters.append("DATE(occurred_at) <= :end_date")
        params["end_date"] = end_date

    filter_clause = ""
    if filters:
        filter_clause = " AND " + " AND ".join(filters)

    base_query = f"""
        SELECT event_id, site_id, occurred_at, code, description
        FROM evi_events
        WHERE 1=1{filter_clause}
        ORDER BY occurred_at DESC
        LIMIT :limit OFFSET :offset
    """
    count_query = f"""
        SELECT COUNT(*) FROM evi_events WHERE 1=1{filter_clause}
    """

    params_with_pagination = {**params, "limit": page_size, "offset": (page - 1) * page_size}
    result = await db.execute(text(base_query), params_with_pagination)
    rows = result.mappings().all()
    count_result = await db.execute(text(count_query), params)
    total = count_result.scalar_one_or_none() or 0

    items = [EviResponse(**row) for row in rows]
    return PaginatedResponse[EviResponse](total=total, page=page, page_size=page_size, items=items)


@router.get("/", response_model=PaginatedResponse[EviResponse])
@cache_response(expire=settings.cache_ttl_evi, namespace="evi")
async def list_evi(
    site_id: Optional[int] = Query(default=None, description="Filtrer par identifiant de site"),
    start_date: Optional[date] = Query(default=None, description="Date de début"),
    end_date: Optional[date] = Query(default=None, description="Date de fin"),
    page: int = Query(default=1, ge=1, description="Numéro de page"),
    page_size: int = Query(default=50, ge=1, le=500, description="Taille de la page"),
    db: AsyncSession = Depends(get_db),
) -> PaginatedResponse[EviResponse]:
    return await _fetch_evi(db, site_id, start_date, end_date, page, page_size)

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import cache_response
from app.database import get_db
from app.dependencies import verify_token
from app.schemas import KpiResponse, PaginatedResponse
from app.settings import get_settings

router = APIRouter(prefix="/kpis", tags=["kpis"], dependencies=[Depends(verify_token)])
settings = get_settings()


async def _fetch_kpis(
    db: AsyncSession,
    site_id: Optional[int],
    start_date: Optional[date],
    end_date: Optional[date],
    page: int,
    page_size: int,
) -> PaginatedResponse[KpiResponse]:
    filters: List[str] = []
    params: dict = {}

    if site_id is not None:
        filters.append("site_id = :site_id")
        params["site_id"] = site_id
    if start_date is not None:
        filters.append("period_start >= :start_date")
        params["start_date"] = start_date
    if end_date is not None:
        filters.append("period_end <= :end_date")
        params["end_date"] = end_date

    filter_clause = ""
    if filters:
        filter_clause = " AND " + " AND ".join(filters)

    base_query = f"""
        SELECT id, site_id, metric, value, period_start, period_end
        FROM kpis
        WHERE 1=1{filter_clause}
        ORDER BY period_start DESC
        LIMIT :limit OFFSET :offset
    """
    count_query = f"""
        SELECT COUNT(*) FROM kpis WHERE 1=1{filter_clause}
    """

    params_with_pagination = {**params, "limit": page_size, "offset": (page - 1) * page_size}
    result = await db.execute(text(base_query), params_with_pagination)
    rows = result.mappings().all()
    count_result = await db.execute(text(count_query), params)
    total = count_result.scalar_one_or_none() or 0

    items = [KpiResponse(**row) for row in rows]
    return PaginatedResponse[KpiResponse](total=total, page=page, page_size=page_size, items=items)


@router.get("/", response_model=PaginatedResponse[KpiResponse])
@cache_response(expire=settings.cache_ttl_kpis, namespace="kpis")
async def list_kpis(
    site_id: Optional[int] = Query(default=None, description="Filtrer par identifiant de site"),
    start_date: Optional[date] = Query(default=None, description="Date de début de la période"),
    end_date: Optional[date] = Query(default=None, description="Date de fin de la période"),
    page: int = Query(default=1, ge=1, description="Numéro de page"),
    page_size: int = Query(default=50, ge=1, le=500, description="Taille de la page"),
    db: AsyncSession = Depends(get_db),
) -> PaginatedResponse[KpiResponse]:
    return await _fetch_kpis(db, site_id, start_date, end_date, page, page_size)

from datetime import date, datetime
from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel


class KpiResponse(BaseModel):
    id: Optional[int] = None
    site_id: Optional[int] = None
    metric: str
    value: float
    period_start: Optional[date] = None
    period_end: Optional[date] = None


class AggregatedKpiResponse(BaseModel):
    site_id: Optional[int] = None
    period_start: date
    period_end: date
    session_count: int
    total_energy_kwh: float | None = None
    average_session_kwh: float | None = None
    total_session_hours: float | None = None


class SessionResponse(BaseModel):
    session_id: Optional[int] = None
    site_id: Optional[int] = None
    started_at: datetime
    ended_at: Optional[datetime] = None
    status: Optional[str] = None
    energy_kwh: Optional[float] = None


class EviResponse(BaseModel):
    event_id: Optional[int] = None
    site_id: Optional[int] = None
    occurred_at: datetime
    code: Optional[str] = None
    description: Optional[str] = None


type ItemT = TypeVar("ItemT", bound=BaseModel)


class PaginatedResponse(BaseModel, Generic[ItemT]):
    total: int
    page: int
    page_size: int
    items: List[ItemT]

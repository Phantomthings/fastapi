from datetime import date
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from .api_client import api_get, get_api_config


@st.cache_data(ttl=lambda: get_api_config().cache_ttl, show_spinner=False)
def _fetch_sessions(site_id: Optional[int], start: Optional[date], end: Optional[date], cache_version: str) -> pd.DataFrame:
    params: Dict[str, Any] = {
        "page": 1,
        "page_size": 2000,
        "cache_version": cache_version,
    }
    if site_id is not None:
        params["site_id"] = site_id
    if start is not None:
        params["start_date"] = start
    if end is not None:
        params["end_date"] = end

    payload = api_get("/sessions", params=params)
    items = payload.get("items") or payload.get("data") or []
    df = pd.DataFrame(items)
    return df


def fetch_sessions(site_id: Optional[int], start: Optional[date], end: Optional[date]) -> pd.DataFrame:
    config = get_api_config()
    cache_version = config.cache_version
    try:
        return _fetch_sessions(site_id, start, end, cache_version)
    except Exception as exc:  # pragma: no cover - UI feedback only
        st.error(f"Erreur lors du chargement des sessions : {exc}")
        return pd.DataFrame()

import os
from typing import Any, Dict, Optional

import requests
import streamlit as st


class ApiConfig:
    def __init__(self) -> None:
        secrets_api = st.secrets.get("api", {}) if hasattr(st, "secrets") else {}
        self.base_url: str = str(
            secrets_api.get("base_url")
            or os.getenv("API_BASE_URL")
            or ""
        ).rstrip("/")
        self.token: str = str(
            secrets_api.get("token")
            or os.getenv("API_TOKEN")
            or ""
        )
        self.cache_ttl: int = int(
            secrets_api.get("cache_ttl", os.getenv("API_CACHE_TTL", 900))
        )
        self.cache_version: str = str(
            secrets_api.get("cache_version")
            or os.getenv("API_CACHE_VERSION")
            or "1"
        )

    def headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers


def get_api_config() -> ApiConfig:
    return ApiConfig()


def _raise_for_status(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:  # pragma: no cover - simple passthrough
        detail = exc.response.text if exc.response is not None else str(exc)
        raise requests.HTTPError(detail) from exc


def api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = get_api_config()
    if not config.base_url:
        raise RuntimeError(
            "Aucune URL d'API configurée. Définir API_BASE_URL ou secrets['api']['base_url']."
        )

    url = f"{config.base_url}{path}"
    response = requests.get(url, headers=config.headers(), params=params, timeout=30)
    _raise_for_status(response)
    data = response.json()
    if isinstance(data, dict):
        return data
    return {"data": data}

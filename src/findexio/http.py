from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_TIMEOUT = (10, 60)

def build_session(*, user_agent: str = "Findexio/0.1", pool_connections: int = 16, pool_maxsize: int = 16) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "User-Agent": user_agent})

    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_connections, pool_maxsize=pool_maxsize)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

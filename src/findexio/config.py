from __future__ import annotations

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # dotenv je lokálny komfort; v Railway sa typicky nepoužíva
    pass


def _normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql+psycopg://"):
        url = "postgresql://" + url[len("postgresql+psycopg://"):]
    return url


def get_db_dsn() -> str:
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return _normalize_database_url(db_url)

    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB")

    if not all([user, password, host, port, db]):
        missing = [k for k in ["PG_USER", "PG_PASSWORD", "PG_HOST", "PG_PORT", "PG_DB"] if not os.getenv(k)]
        raise RuntimeError(f"Database config missing. Set DATABASE_URL or: {', '.join(missing)}")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


DB_DSN = get_db_dsn()
RUZ_API_BASE = os.getenv("RUZ_API_BASE", "https://www.registeruz.sk/cruz-public").rstrip("/")
RPO_S3_ENDPOINT = os.getenv(
    "RPO_S3_ENDPOINT",
    "https://frkqbrydxwdp.compat.objectstorage.eu-frankfurt-1.oraclecloud.com",
).rstrip("/")

SD_API_BASE = os.getenv(
    "SD_API_BASE",
    "https://datahub.ekosystem.slovensko.digital/api/data/rpo",
).rstrip("/")

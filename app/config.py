"""运行期配置。

所有连接信息均来自环境变量，便于在 docker-compose 中注入。
"""
from __future__ import annotations

import os


def _database_url() -> str:
    explicit = os.getenv("DATABASE_URL")
    if explicit:
        return explicit

    user = os.getenv("POSTGRES_USER", "evac")
    password = os.getenv("POSTGRES_PASSWORD", "evac")
    db = os.getenv("POSTGRES_DB", "evac")
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"


DATABASE_URL: str = _database_url()

# SQLAlchemy 连接池在多进程（uvicorn 多 worker / multiprocessing 验收）下
# 会各自建池，配合 staticPool 以外的默认 QueuePool 安全使用。
DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "5"))
DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "5"))

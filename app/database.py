"""SQLAlchemy 引擎与会话工厂。"""
from __future__ import annotations

import logging
from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import DB_MAX_OVERFLOW, DB_POOL_SIZE, DATABASE_URL
from .request_context import current_request_id

logger = logging.getLogger(__name__)

engine = create_engine(
    DATABASE_URL,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_pre_ping=True,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_session() -> Iterator[Session]:
    """FastAPI 依赖：每个请求一个事务会话。

    会话在请求结束前沿用同一请求上下文：``session.info`` 记录当前
    request_id，会话开/关日志经日志过滤器自动携带同一标识，与入口、
    业务日志串到同一次调用。``finally`` 保证正常返回与异常退出都会
    关闭会话，上下文不泄漏到下一个请求。
    """
    session = SessionLocal(info={"request_id": current_request_id() or "-"})
    logger.info("db session opened")
    try:
        yield session
    finally:
        session.close()
        logger.info("db session closed")

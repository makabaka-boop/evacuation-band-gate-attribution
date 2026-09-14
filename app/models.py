"""ORM 模型。

核心不变量：
* ``band_first_seen`` 以 ``band_id`` 为主键 —— 数据库层保证同一腕带全局
  只有一条首次通过事实；
* 另有 ``event_id`` 的唯一约束，使每笔成功提交的扫描事件恰好落一行；
* ``idempotent_requests`` 以 ``event_id`` 为主键，保存规范化载荷与原响应，
  实现跨重启的持久化幂等。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class BandFirstSeen(Base):
    __tablename__ = "band_first_seen"

    #: 腕带号本身即主键：同一腕带有且仅有一条归属事实。
    band_id: Mapped[str] = mapped_column(String(128), primary_key=True)

    #: 赢得首次归属的那笔扫描事件（也唯一，作为事实溯源）。
    event_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    gate_id: Mapped[str] = mapped_column(String(128), nullable=False)

    #: 客户端声明的扫描时刻；归属本身由“谁先提交成功”决定，不按此时间倒排。
    scanned_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    #: 服务器确认该事实落库的时间（事务提交顺序的物理见证）。
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class IdempotentRequest(Base):
    __tablename__ = "idempotent_requests"

    #: 客户端提供的事件/幂等键。
    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)

    #: 规范化后的原始载荷，用于检测同键不同载荷冲突。
    request_payload: Mapped[dict] = mapped_column(JSON, nullable=False)

    #: 首次成功处理时返回的完整响应，重放时原样返回。
    response_body: Mapped[dict] = mapped_column(JSON, nullable=False)

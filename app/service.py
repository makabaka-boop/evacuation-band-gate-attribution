"""扫描上报的核心事务逻辑。

并发设计（PostgreSQL READ COMMITTED）
-------------------------------------

1. 同一个 ``event_id`` 的并发请求先取一把事务级咨询锁
   (``pg_advisory_xact_lock``，键为 event_id 的 64 位哈希)。它在多进程间
   生效，把“重放/同键冲突”的判定串行化，锁随事务提交/回滚自动释放。
2. 腕带归属则由 ``band_first_seen`` 的主键（``band_id``）竞争决定：
   用 ``INSERT ... ON CONFLICT DO NOTHING`` 尝试插入；受主键阻塞的并发
   事务会在获胜事务提交后唤醒，发现冲突行已存在，于是走“读取既有事实”
   分支。因此跨请求、跨进程恰有一个 ``first_seen``。
3. 幂等记录与归属事实在**同一事务**中落库：要么一同可见，要么一同回滚，
   不会出现“响应已重放但事实缺失”的中间态。

顺序不采用客户端 ``scanned_at`` —— 谁的事务先提交，谁永久成为首次闸机。
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import BandFirstSeen, IdempotentRequest
from .schemas import ScanRequest, ScanResponse


class PayloadConflictError(Exception):
    """同一 event_id 携带了不同的载荷 —— HTTP 409。"""

    def __init__(self, original_payload: dict) -> None:
        self.original_payload = original_payload
        super().__init__("idempotency key reused with a different payload")


def _advisory_key(event_id: str) -> int:
    """把任意长度的 event_id 散列成 pg_advisory_lock 使用的有符号 bigint。"""
    digest = hashlib.blake2b(event_id.encode("utf-8"), digest_size=8).digest()
    value = int.from_bytes(digest, "big", signed=False)
    # 映射到 PostgreSQL bigint 的有符号范围。
    return value - (1 << 64) if value >= (1 << 63) else value


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _build_response(fact: BandFirstSeen, event_id: str) -> ScanResponse:
    return ScanResponse(
        event_id=event_id,
        band_id=fact.band_id,
        gate_id=fact.gate_id,
        scanned_at=fact.scanned_at,
        first_gate_id=fact.gate_id,
        first_seen_at=fact.created_at,
        result="first_seen" if event_id == fact.event_id else "already_seen",
    )


def submit_scan(session: Session, request: ScanRequest) -> ScanResponse:
    """在调用方提供的事务会话中提交一次扫描。

    成功时由外层（请求结束/显式 commit）提交；抛出任何异常都会回滚。
    返回的响应即权威响应，同 event_id 的重放永远返回它的逐字副本。
    """
    payload = request.canonical_payload()
    lock_key = _advisory_key(request.event_id)

    # 1) 同 event_id 串行化（跨进程，随事务结束自动释放）。
    session.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": lock_key})

    # 2) 幂等快查：已落库的 event —— 重放或冲突。
    existing = session.get(IdempotentRequest, request.event_id)
    if existing is not None:
        if existing.request_payload != payload:
            raise PayloadConflictError(existing.request_payload)
        return ScanResponse.model_validate(existing.response_body)

    # 3) 竞争腕带的唯一首次事实。不同 event_id 的并发请求不互斥咨询锁，
    #    全部落到下面的主键竞争上裁决。
    insert_stmt = (
        pg_insert(BandFirstSeen)
        .values(
            band_id=request.band_id,
            event_id=request.event_id,
            gate_id=request.gate_id,
            scanned_at=request.scanned_at,
            created_at=_now(),
        )
        .on_conflict_do_nothing(index_elements=[BandFirstSeen.band_id])
        .returning(
            BandFirstSeen.band_id,
            BandFirstSeen.event_id,
            BandFirstSeen.gate_id,
            BandFirstSeen.scanned_at,
            BandFirstSeen.created_at,
        )
    )
    row = session.execute(insert_stmt).first()

    if row is not None:
        fact = BandFirstSeen(
            band_id=row.band_id,
            event_id=row.event_id,
            gate_id=row.gate_id,
            scanned_at=row.scanned_at,
            created_at=row.created_at,
        )
    else:
        # 已有获胜事务提交：读取同一归属事实。
        fact = session.get(BandFirstSeen, request.band_id)
        assert fact is not None  # 主键冲突意味着它必然存在

    response = _build_response(fact, request.event_id)

    # 4) 幂等记录与事实同一事务写入。event_id 唯一约束兜底：
    #    极端情况下（持锁前他人已提交同 event）在此报冲突，整体回滚，
    #    调用方重试即得到权威响应。
    session.add(
        IdempotentRequest(
            event_id=request.event_id,
            request_payload=payload,
            response_body=response.model_dump(mode="json"),
        )
    )
    try:
        session.flush()
    except IntegrityError as exc:  # pragma: no cover - 极端竞态兜底
        session.rollback()
        stored = session.get(IdempotentRequest, request.event_id)
        if stored is not None:
            if stored.request_payload != payload:
                raise PayloadConflictError(stored.request_payload) from exc
            return ScanResponse.model_validate(stored.response_body)
        raise

    return response


def get_band_fact(session: Session, band_id: str) -> BandFirstSeen | None:
    """返回腕带的唯一首次通过事实；不存在返回 None。"""
    return session.get(BandFirstSeen, band_id)

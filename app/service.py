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

疏散名册
--------
名册复用腕带编号与 ``band_first_seen`` 的既有事实，不触碰归属裁决：

* 创建：``evacuation_rosters.roster_id`` 主键竞争即唯一性裁决，
  ``INSERT ... ON CONFLICT DO NOTHING`` 拿不到 RETURNING 行即 409，
  原名册分毫不动；名册行与成员行在同一事务落库。
* 核对：成员集与 ``band_first_seen`` 的集合差分在**同一条 SELECT**
  （同一事务快照）内完成，汇总数字与未通过明细由同一批行推导，
  必然一致；查询期间提交的扫描只影响后续请求。

闸机巡检
--------
巡检记录只增不改，与扫描、名册完全解耦（故障结论不阻断既有扫描）：

* 提交：``gate_inspections`` 追加一行，``seq`` 由数据库 IDENTITY 生成；
  ``inspection_id`` 唯一约束即重复裁决，``INSERT ... ON CONFLICT DO
  NOTHING`` 拿不到 RETURNING 行即 409，原记录分毫不动。
* 查询：按 ``gate_id`` 取 ``seq`` 最大的一行 —— “最近一次”由数据库
  生成的递增序号（提交顺序）裁决，绝不按客户端 ``checked_at`` 倒排。

闸机增援派驻
------------
派驻记录复用 ``gate_id`` 命名空间，但不读取也不改变巡检结论：

* 派驻：``gate_deployments.deployment_id`` 主键竞争即唯一性裁决，
  ``INSERT ... ON CONFLICT DO NOTHING`` 拿不到 RETURNING 行即 409，
  原派驻分毫不动；新记录 ``arrived_at`` 为 NULL，即“待到岗”。
* 到岗确认：唯一的推进路径是条件更新
  ``UPDATE ... WHERE deployment_id = :id AND arrived_at IS NULL``。
  并发确认在数据库行锁上串行：获胜事务提交后，被阻塞的事务重估条件
  发现 ``arrived_at`` 已非 NULL，拿不到 RETURNING 行 —— 恰有一个
  请求写入到岗时间（200），其余返回 409 且原值分毫不动；确认不存在的
  派驻返回 404。阶段只允许 待到岗 -> 已到岗，无反向路径。
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import (
    BandFirstSeen,
    EvacuationRoster,
    GateDeployment,
    GateInspection,
    IdempotentRequest,
    RosterMember,
)
from .schemas import (
    DeploymentArrivalRequest,
    DeploymentCreateRequest,
    DeploymentResponse,
    GateInspectionStatus,
    InspectionRequest,
    InspectionResponse,
    RosterCheckResponse,
    RosterCreateRequest,
    RosterCreatedResponse,
    ScanRequest,
    ScanResponse,
)

# 业务处理日志：经日志过滤器自动携带当前请求的 request_id，
# 与入口中间件、数据库会话日志串到同一次调用。
logger = logging.getLogger(__name__)


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
            logger.info("scan conflict: event_id=%s", request.event_id)
            raise PayloadConflictError(existing.request_payload)
        logger.info("scan replayed: event_id=%s", request.event_id)
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

    logger.info(
        "scan settled: event_id=%s band_id=%s result=%s",
        request.event_id,
        fact.band_id,
        response.result,
    )
    return response


def get_band_fact(session: Session, band_id: str) -> BandFirstSeen | None:
    """返回腕带的唯一首次通过事实；不存在返回 None。"""
    return session.get(BandFirstSeen, band_id)


class RosterConflictError(Exception):
    """roster_id 已被占用 —— HTTP 409，原名册及其成员保持不变。"""

    def __init__(self, roster_id: str) -> None:
        self.roster_id = roster_id
        super().__init__(f"roster_id already exists: {roster_id}")


def create_roster(session: Session, request: RosterCreateRequest) -> RosterCreatedResponse:
    """在调用方提供的事务会话中创建名册（名册行 + 成员行同一事务落库）。

    ``roster_id`` 的主键竞争即唯一性裁决：插入被已提交（或正在提交）的
    同名册阻塞/顶回时拿不到 RETURNING 行，抛 :class:`RosterConflictError`，
    调用方回滚后原名册分毫不动。成员腕带的非空与去重已在请求层校验。
    """
    stmt = (
        pg_insert(EvacuationRoster)
        .values(
            roster_id=request.roster_id,
            name=request.name,
            created_at=_now(),
        )
        .on_conflict_do_nothing(index_elements=[EvacuationRoster.roster_id])
        .returning(EvacuationRoster.roster_id)
    )
    row = session.execute(stmt).first()
    if row is None:
        logger.info("roster conflict: roster_id=%s", request.roster_id)
        raise RosterConflictError(request.roster_id)

    session.add_all(
        RosterMember(roster_id=request.roster_id, band_id=band_id)
        for band_id in request.band_ids
    )
    session.flush()
    logger.info(
        "roster created: roster_id=%s expected_count=%d",
        request.roster_id,
        len(request.band_ids),
    )
    return RosterCreatedResponse(
        roster_id=request.roster_id,
        name=request.name,
        expected_count=len(request.band_ids),
    )


def check_roster(session: Session, roster_id: str) -> RosterCheckResponse | None:
    """按 roster_id 核对尚未过闸人员；名册不存在返回 None。

    名册成员 LEFT JOIN ``band_first_seen``：单条 SELECT 保证两个集合取自
    同一事务快照，差分（未命中者即未过闸）与汇总数字来自同一批行，必然
    一致；查询期间提交的扫描对本语句不可见，只影响后续请求。名册创建后
    必有至少一名成员，因此零行结果即名册不存在。结果按腕带编号排序。
    """
    stmt = (
        select(
            EvacuationRoster.name.label("roster_name"),
            RosterMember.band_id,
            BandFirstSeen.band_id.label("seen_band_id"),
        )
        .select_from(EvacuationRoster)
        .join(RosterMember, RosterMember.roster_id == EvacuationRoster.roster_id)
        .outerjoin(BandFirstSeen, BandFirstSeen.band_id == RosterMember.band_id)
        .where(EvacuationRoster.roster_id == roster_id)
        .order_by(RosterMember.band_id)
    )
    rows = session.execute(stmt).all()
    if not rows:
        return None

    missing = [row.band_id for row in rows if row.seen_band_id is None]
    expected = len(rows)
    return RosterCheckResponse(
        roster_id=roster_id,
        name=rows[0].roster_name,
        expected_count=expected,
        passed_count=expected - len(missing),
        missing_count=len(missing),
        missing_band_ids=missing,
    )


class InspectionConflictError(Exception):
    """inspection_id 已被占用 —— HTTP 409，原巡检记录保持不变。"""

    def __init__(self, inspection_id: str) -> None:
        self.inspection_id = inspection_id
        super().__init__(f"inspection_id already exists: {inspection_id}")


def _inspection_response(record: GateInspection) -> InspectionResponse:
    return InspectionResponse(
        inspection_id=record.inspection_id,
        gate_id=record.gate_id,
        checked_at=record.checked_at,
        conclusion=record.conclusion,
        notes=record.notes,
        seq=record.seq,
        recorded_at=record.created_at,
    )


def submit_inspection(
    session: Session, request: InspectionRequest
) -> InspectionResponse:
    """在调用方提供的事务会话中追加一条巡检记录。

    追加即插入：``seq`` 由数据库 IDENTITY 生成，递增顺序即提交顺序。
    ``inspection_id`` 的唯一约束即重复裁决：插入被已提交（或正在提交）
    的同号记录顶回时拿不到 RETURNING 行，抛 :class:`InspectionConflictError`，
    调用方回滚后原记录分毫不动。标识/闸机号空白、无时区时间与超长备注
    已在请求层（Pydantic）以 422 拒绝，根本不到数据库。
    """
    stmt = (
        pg_insert(GateInspection)
        .values(
            inspection_id=request.inspection_id,
            gate_id=request.gate_id,
            checked_at=request.checked_at,
            conclusion=request.conclusion,
            notes=request.notes,
            created_at=_now(),
        )
        .on_conflict_do_nothing(index_elements=[GateInspection.inspection_id])
        .returning(GateInspection.seq, GateInspection.created_at)
    )
    row = session.execute(stmt).first()
    if row is None:
        logger.info("inspection conflict: inspection_id=%s", request.inspection_id)
        raise InspectionConflictError(request.inspection_id)
    logger.info(
        "inspection recorded: inspection_id=%s gate_id=%s conclusion=%s",
        request.inspection_id,
        request.gate_id,
        request.conclusion,
    )
    return InspectionResponse(
        inspection_id=request.inspection_id,
        gate_id=request.gate_id,
        checked_at=request.checked_at,
        conclusion=request.conclusion,
        notes=request.notes,
        seq=row.seq,
        recorded_at=row.created_at,
    )


def get_latest_inspection(session: Session, gate_id: str) -> GateInspectionStatus | None:
    """按 gate_id 返回最近一次巡检记录与闸机状态；无记录返回 None。

    “最近一次”按数据库生成的递增序号 ``seq``（追加/提交顺序）裁决，
    不按客户端 ``checked_at`` 倒排 —— 乱序时钟不会改变最新记录。
    故障结论只反映在此状态查询中，不影响扫描归属与名册核对。
    """
    stmt = (
        select(GateInspection)
        .where(GateInspection.gate_id == gate_id)
        .order_by(GateInspection.seq.desc())
        .limit(1)
    )
    record = session.execute(stmt).scalar_one_or_none()
    if record is None:
        return None
    return GateInspectionStatus(
        gate_id=record.gate_id,
        status=record.conclusion,
        latest_inspection=_inspection_response(record),
    )


class DeploymentConflictError(Exception):
    """deployment_id 已被占用 —— HTTP 409，原派驻记录保持不变。"""

    def __init__(self, deployment_id: str) -> None:
        self.deployment_id = deployment_id
        super().__init__(f"deployment_id already exists: {deployment_id}")


class ArrivalConflictError(Exception):
    """派驻已被确认到岗 —— HTTP 409，先到岗时间保持不变。"""

    def __init__(self, deployment_id: str, arrived_at: datetime) -> None:
        self.deployment_id = deployment_id
        self.arrived_at = arrived_at
        super().__init__(f"deployment already confirmed: {deployment_id}")


def _deployment_response(record: GateDeployment) -> DeploymentResponse:
    return DeploymentResponse(
        deployment_id=record.deployment_id,
        responder_id=record.responder_id,
        gate_id=record.gate_id,
        deployed_at=record.deployed_at,
        arrived_at=record.arrived_at,
        phase="arrived" if record.arrived_at is not None else "pending",
        recorded_at=record.created_at,
    )


def create_deployment(
    session: Session, request: DeploymentCreateRequest
) -> DeploymentResponse:
    """在调用方提供的事务会话中形成一条“待到岗”派驻记录。

    ``deployment_id`` 的主键竞争即唯一性裁决：插入被已提交（或正在提交）
    的同号派驻顶回时拿不到 RETURNING 行，抛 :class:`DeploymentConflictError`，
    调用方回滚后原派驻分毫不动。空白标识/人员号/闸机号与无时区时间已在
    请求层（Pydantic）以 422 拒绝，根本不到数据库。
    """
    stmt = (
        pg_insert(GateDeployment)
        .values(
            deployment_id=request.deployment_id,
            responder_id=request.responder_id,
            gate_id=request.gate_id,
            deployed_at=request.deployed_at,
            arrived_at=None,
            created_at=_now(),
        )
        .on_conflict_do_nothing(index_elements=[GateDeployment.deployment_id])
        .returning(GateDeployment.created_at)
    )
    row = session.execute(stmt).first()
    if row is None:
        logger.info("deployment conflict: deployment_id=%s", request.deployment_id)
        raise DeploymentConflictError(request.deployment_id)
    logger.info(
        "deployment created: deployment_id=%s gate_id=%s",
        request.deployment_id,
        request.gate_id,
    )
    return DeploymentResponse(
        deployment_id=request.deployment_id,
        responder_id=request.responder_id,
        gate_id=request.gate_id,
        deployed_at=request.deployed_at,
        arrived_at=None,
        phase="pending",
        recorded_at=row.created_at,
    )


def confirm_arrival(
    session: Session, deployment_id: str, request: DeploymentArrivalRequest
) -> DeploymentResponse | None:
    """把“待到岗”派驻推进为“已到岗”；派驻不存在返回 None。

    并发确认由数据库条件更新裁决：
    ``UPDATE ... WHERE deployment_id = :id AND arrived_at IS NULL`` 在
    行锁上串行 —— 获胜事务提交后，被阻塞的事务按 READ COMMITTED 重估
    条件，发现 ``arrived_at`` 已非 NULL，于是拿不到 RETURNING 行。因此
    跨请求、跨进程恰有一个确认写入到岗时间；落败事务转而读取同一行，
    抛 :class:`ArrivalConflictError`（409），先到岗时间分毫不动。
    """
    stmt = (
        update(GateDeployment)
        .where(GateDeployment.deployment_id == deployment_id)
        .where(GateDeployment.arrived_at.is_(None))
        .values(arrived_at=request.arrived_at)
        .returning(
            GateDeployment.deployment_id,
            GateDeployment.responder_id,
            GateDeployment.gate_id,
            GateDeployment.deployed_at,
            GateDeployment.arrived_at,
            GateDeployment.created_at,
        )
        .execution_options(synchronize_session=False)
    )
    row = session.execute(stmt).first()
    if row is not None:
        logger.info("arrival confirmed: deployment_id=%s", deployment_id)
        return DeploymentResponse(
            deployment_id=row.deployment_id,
            responder_id=row.responder_id,
            gate_id=row.gate_id,
            deployed_at=row.deployed_at,
            arrived_at=row.arrived_at,
            phase="arrived",
            recorded_at=row.created_at,
        )

    # 条件更新未命中：要么派驻不存在（404），要么已被确认（409）。
    # READ COMMITTED 下这条 SELECT 取新快照，能看到刚提交的获胜确认。
    existing = session.execute(
        select(GateDeployment).where(GateDeployment.deployment_id == deployment_id)
    ).scalar_one_or_none()
    if existing is None:
        return None
    assert existing.arrived_at is not None  # 条件更新未命中即已确认
    logger.info("arrival conflict: deployment_id=%s", deployment_id)
    raise ArrivalConflictError(deployment_id, existing.arrived_at)


def get_deployment(session: Session, deployment_id: str) -> DeploymentResponse | None:
    """按 deployment_id 返回派驻事实与当前阶段；不存在返回 None。"""
    record = session.get(GateDeployment, deployment_id)
    if record is None:
        return None
    return _deployment_response(record)

"""ORM 模型。

核心不变量：
* ``band_first_seen`` 以 ``band_id`` 为主键 —— 数据库层保证同一腕带全局
  只有一条首次通过事实；
* 另有 ``event_id`` 的唯一约束，使每笔成功提交的扫描事件恰好落一行；
* ``idempotent_requests`` 以 ``event_id`` 为主键，保存规范化载荷与原响应，
  实现跨重启的持久化幂等；
* ``evacuation_rosters`` 以 ``roster_id`` 为主键，``roster_members`` 以
  ``(roster_id, band_id)`` 为联合主键 —— 名册全局唯一、名册内腕带去重，
  核对时直接复用 ``band_first_seen`` 的既有事实，不复制、不改写；
* ``gate_inspections`` 只增不改（追加式）：数据库生成的递增 ``seq`` 主键
  是“最近一次”的唯一裁决依据（与客户端检查时间无关），``inspection_id``
  的唯一约束裁决重复提交（409），原记录分毫不动；
* ``gate_deployments`` 以 ``deployment_id`` 为主键 —— 重复派驻由主键竞争
  裁决（409）；``arrived_at`` 只能从 NULL 被条件更新写入一次，阶段推进
  （待到岗 -> 已到岗）由数据库条件更新裁决并发确认，先到岗时间不被覆盖。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, BigInteger, DateTime, ForeignKey, Identity, String
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


class EvacuationRoster(Base):
    __tablename__ = "evacuation_rosters"

    #: 负责人提供的名册 ID 本身即主键：重复创建由主键竞争裁决（409）。
    roster_id: Mapped[str] = mapped_column(String(128), primary_key=True)

    #: 名册名称（如 “3F 东侧车间”），仅作展示，不参与裁决。
    name: Mapped[str] = mapped_column(String(256), nullable=False)

    #: 名册落库时间。
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class RosterMember(Base):
    __tablename__ = "roster_members"

    #: 所属名册；随名册删除级联清理。
    roster_id: Mapped[str] = mapped_column(
        ForeignKey("evacuation_rosters.roster_id", ondelete="CASCADE"),
        primary_key=True,
    )

    #: 应到腕带号。联合主键兜底名册内去重（请求层已先行校验）。
    band_id: Mapped[str] = mapped_column(String(128), primary_key=True)


class GateInspection(Base):
    """闸机巡检记录：只增不改的追加式事实表。"""

    __tablename__ = "gate_inspections"

    #: 数据库生成的递增序号（IDENTITY）：追加（提交）顺序的物理见证。
    #: “最近一次记录”完全由它裁决 —— 与客户端声明的检查时间无关，
    #: 乱序时钟无法反客为主。
    seq: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)

    #: 值守人员提供的巡检标识。唯一约束即重复裁决：同号提交拿不到
    #: RETURNING 行即 409，原记录保持不变。
    inspection_id: Mapped[str] = mapped_column(
        String(128), nullable=False, unique=True
    )

    #: 复用扫描载荷的闸机号（同一命名空间）；按它检索该闸机的巡检历史。
    gate_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)

    #: 客户端声明的检查时刻（必须带时区）；仅作事实记录，不参与“最新”裁决。
    checked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    #: 检查结论：available / faulty。故障结论只影响状态查询，不阻断既有扫描。
    conclusion: Mapped[str] = mapped_column(String(16), nullable=False)

    #: 可选备注（长度上限见 schemas.INSPECTION_NOTES_MAX_LENGTH）。
    notes: Mapped[str | None] = mapped_column(String(500), nullable=True)

    #: 服务器确认该记录落库的时间。
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class GateDeployment(Base):
    """闸机增援派驻记录：阶段只沿 待到岗 -> 已到岗 单向推进。"""

    __tablename__ = "gate_deployments"

    #: 调度员提供的派驻标识本身即主键：重复派驻由主键竞争裁决（409），
    #: 原派驻记录分毫不动。
    deployment_id: Mapped[str] = mapped_column(String(128), primary_key=True)

    #: 增援人员号。
    responder_id: Mapped[str] = mapped_column(String(128), nullable=False)

    #: 复用扫描/巡检载荷的闸机号（同一命名空间）；派驻记录既不读取也不
    #: 改变巡检结论，二者完全解耦。
    gate_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)

    #: 调度员声明的派驻时刻（必须带时区）；仅作事实记录。
    deployed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    #: 增援人员声明的到岗时刻（必须带时区）。NULL 即“待到岗”；只能由
    #: 条件更新（``arrived_at IS NULL``）写入一次 —— 并发确认在数据库
    #: 行锁上串行，恰有一个请求落定到岗时间，其余 409 且不得覆盖原值。
    arrived_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    #: 服务器确认派驻记录落库的时间。
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

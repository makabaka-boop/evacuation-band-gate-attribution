"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)


class ScanRequest(BaseModel):
    """一次闸机扫描上报。"""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    band_id: str = Field(min_length=1)
    gate_id: str = Field(min_length=1)

    #: 必须携带时区偏移，例如 2026-09-14T10:00:00+08:00。
    scanned_at: datetime

    #: 客户端提交的 scanned_at 原文。幂等按“完整载荷”比较，因此时区写法
    #: 本身也是载荷的一部分：+08:00 与其等价的 Z 时刻属于不同载荷，
    #: 同一 event_id 重放时必须以 409 拒绝。
    _scanned_at_raw: str = PrivateAttr(default="")

    @field_validator("scanned_at")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("scanned_at must include a timezone offset")
        return value

    @model_validator(mode="wrap")
    @classmethod
    def _capture_raw_scanned_at(cls, data: Any, handler) -> "ScanRequest":
        raw: str | None = None
        if isinstance(data, dict) and "scanned_at" in data:
            value = data["scanned_at"]
            raw = value if isinstance(value, str) else value.isoformat()
        request = handler(data)
        if raw is not None:
            request._scanned_at_raw = raw
        return request

    def canonical_payload(self) -> dict[str, str]:
        """用于幂等比较的规范化载荷（逐字段、逐字符）。

        ``scanned_at`` 保留客户端原始写法而不做 UTC 归一化：只有完整载荷
        完全一致的重放才返回原响应；换一种时区写法（如 ``+08:00`` 改写为
        等价的 ``Z``）即判定为同键不同载荷，返回 409 且不改变归属。

        注意归属顺序从不使用该时间，只由事务提交先后决定。
        """
        if not self._scanned_at_raw:  # pragma: no cover - wrap 校验器总会填充
            self._scanned_at_raw = self.scanned_at.isoformat()
        return {
            "event_id": self.event_id,
            "band_id": self.band_id,
            "gate_id": self.gate_id,
            "scanned_at": self._scanned_at_raw,
        }


class ScanResponse(BaseModel):
    """扫描处理结果。first_seen / already_seen 返回体结构相同，便于清点方使用。"""

    event_id: str
    band_id: str
    gate_id: str
    scanned_at: datetime
    first_gate_id: str
    first_seen_at: datetime
    result: Literal["first_seen", "already_seen"]


class BandFact(BaseModel):
    """按 band_id 查询到的唯一首次通过事实。"""

    band_id: str
    event_id: str
    gate_id: str
    scanned_at: datetime
    created_at: datetime


class RosterCreateRequest(BaseModel):
    """创建疏散名册：roster_id/name 非空白且唯一，band_ids 须非空且已去重。"""

    model_config = ConfigDict(extra="forbid")

    #: 负责人提供的名册标识；纯空白不可辨识，按非法请求拒绝。含斜杠的
    #: 层级式标识允许创建（核对路由用 :path 接收，见 main.py）。
    roster_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=256)

    #: 应到腕带列表。空列表、空/空白条目或重复腕带都属于非法请求（422），
    #: 由负责人在提交前去重。
    band_ids: list[str] = Field(min_length=1)

    @field_validator("roster_id")
    @classmethod
    def _require_non_blank_roster_id(cls, value: str) -> str:
        # 纯空白标识没有任何可辨识信息 —— 在持久化前即拒绝（422）。
        if not value.strip():
            raise ValueError("roster_id must not be blank")
        return value

    @field_validator("name")
    @classmethod
    def _require_non_blank_name(cls, value: str) -> str:
        # 纯空白名称没有任何可辨识信息，按非法创建请求拒绝，不予保留。
        if not value.strip():
            raise ValueError("name must not be blank")
        return value

    @field_validator("band_ids")
    @classmethod
    def _require_deduplicated_band_ids(cls, value: list[str]) -> list[str]:
        seen: set[str] = set()
        for band_id in value:
            if not 1 <= len(band_id) <= 128:
                raise ValueError("band_id entries must be 1..128 characters")
            # 纯空白腕带号不是有效成员：拒绝，避免其虚增应到/未通过人数。
            if not band_id.strip():
                raise ValueError("band_id entries must not be blank")
            if band_id in seen:
                raise ValueError("band_ids must be deduplicated before submission")
            seen.add(band_id)
        return value


class RosterCreatedResponse(BaseModel):
    """名册创建结果。"""

    roster_id: str
    name: str
    expected_count: int


class RosterCheckResponse(BaseModel):
    """名册核对结果。

    汇总数字（应到/已通过/未通过）与未通过明细来自同一事务快照的同一批
    行，二者必然一致；``missing_band_ids`` 按腕带编号稳定排序。
    """

    roster_id: str
    name: str
    expected_count: int
    passed_count: int
    missing_count: int
    missing_band_ids: list[str]


#: 巡检备注的最大长度；超长备注在入库前以 422 拒绝，不留残行。
INSPECTION_NOTES_MAX_LENGTH = 500

#: 检查结论的合法取值：闸机可用 / 故障。
InspectionConclusion = Literal["available", "faulty"]


class InspectionRequest(BaseModel):
    """一次闸机巡检上报（值守人员提交）。

    ``gate_id`` 复用扫描载荷的闸机号命名空间；``checked_at`` 必须带时区
    偏移，但它只作事实记录 —— “最近一次”由数据库生成的递增序号裁决。
    """

    model_config = ConfigDict(extra="forbid")

    inspection_id: str = Field(min_length=1, max_length=128)
    gate_id: str = Field(min_length=1, max_length=128)

    #: 必须携带时区偏移，例如 2026-09-14T08:30:00+08:00。
    checked_at: datetime

    conclusion: InspectionConclusion

    #: 可选备注；超长（> INSPECTION_NOTES_MAX_LENGTH）在入库前以 422 拒绝。
    notes: str | None = Field(default=None, max_length=INSPECTION_NOTES_MAX_LENGTH)

    @field_validator("inspection_id")
    @classmethod
    def _require_non_blank_inspection_id(cls, value: str) -> str:
        # 纯空白标识没有任何可辨识信息 —— 在持久化前即拒绝（422）。
        if not value.strip():
            raise ValueError("inspection_id must not be blank")
        return value

    @field_validator("gate_id")
    @classmethod
    def _require_non_blank_gate_id(cls, value: str) -> str:
        # 纯空白闸机号无法对应任何物理闸机 —— 在持久化前即拒绝（422）。
        if not value.strip():
            raise ValueError("gate_id must not be blank")
        return value

    @field_validator("checked_at")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("checked_at must include a timezone offset")
        return value


class InspectionResponse(BaseModel):
    """巡检记录落库结果（含数据库生成的递增序号 ``seq``）。"""

    inspection_id: str
    gate_id: str
    checked_at: datetime
    conclusion: InspectionConclusion
    notes: str | None
    seq: int
    recorded_at: datetime


class GateInspectionStatus(BaseModel):
    """按 gate_id 查询到的最近一次巡检记录及闸机可用/故障状态。

    ``status`` 派生自最近一次记录的结论；``latest_inspection`` 即该记录
    本体。“最近一次”按数据库生成的递增 ``seq``（追加顺序）裁决，不按
    客户端 ``checked_at`` 倒排。
    """

    gate_id: str
    status: InspectionConclusion
    latest_inspection: InspectionResponse


#: 派驻阶段的合法取值：待到岗（已派驻待确认）/ 已到岗（到岗已确认）。
DeploymentPhase = Literal["pending", "arrived"]


class DeploymentCreateRequest(BaseModel):
    """调度员提交的一次闸机增援派驻。

    ``gate_id`` 复用扫描/巡检载荷的闸机号命名空间；``deployed_at`` 必须
    带时区偏移。提交成功即形成“待到岗”记录。
    """

    model_config = ConfigDict(extra="forbid")

    deployment_id: str = Field(min_length=1, max_length=128)
    responder_id: str = Field(min_length=1, max_length=128)
    gate_id: str = Field(min_length=1, max_length=128)

    #: 必须携带时区偏移，例如 2026-09-15T09:00:00+08:00。
    deployed_at: datetime

    @field_validator("deployment_id")
    @classmethod
    def _require_non_blank_deployment_id(cls, value: str) -> str:
        # 纯空白标识没有任何可辨识信息 —— 在持久化前即拒绝（422）。
        if not value.strip():
            raise ValueError("deployment_id must not be blank")
        return value

    @field_validator("responder_id")
    @classmethod
    def _require_non_blank_responder_id(cls, value: str) -> str:
        # 纯空白人员号无法对应任何增援人员 —— 在持久化前即拒绝（422）。
        if not value.strip():
            raise ValueError("responder_id must not be blank")
        return value

    @field_validator("gate_id")
    @classmethod
    def _require_non_blank_gate_id(cls, value: str) -> str:
        # 纯空白闸机号无法对应任何物理闸机 —— 在持久化前即拒绝（422）。
        if not value.strip():
            raise ValueError("gate_id must not be blank")
        return value

    @field_validator("deployed_at")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("deployed_at must include a timezone offset")
        return value


class DeploymentArrivalRequest(BaseModel):
    """增援人员按 deployment_id 提交的到岗确认（``arrived_at`` 必须带时区）。"""

    model_config = ConfigDict(extra="forbid")

    #: 必须携带时区偏移，例如 2026-09-15T09:07:00+08:00。
    arrived_at: datetime

    @field_validator("arrived_at")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("arrived_at must include a timezone offset")
        return value


class DeploymentResponse(BaseModel):
    """派驻事实本体与当前阶段（待到岗 pending / 已到岗 arrived）。

    ``arrived_at`` 为 NULL 时阶段为 ``pending``；首个成功的到岗确认把它
    落定后阶段为 ``arrived``，此后任何确认都不得改写该值。
    """

    deployment_id: str
    responder_id: str
    gate_id: str
    deployed_at: datetime
    arrived_at: datetime | None
    phase: DeploymentPhase
    recorded_at: datetime

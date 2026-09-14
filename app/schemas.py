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

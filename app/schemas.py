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

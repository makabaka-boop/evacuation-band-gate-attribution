"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ScanRequest(BaseModel):
    """一次闸机扫描上报。"""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    band_id: str = Field(min_length=1)
    gate_id: str = Field(min_length=1)

    #: 必须携带时区偏移，例如 2026-09-14T10:00:00+08:00。
    scanned_at: datetime

    @field_validator("scanned_at")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("scanned_at must include a timezone offset")
        return value

    def canonical_payload(self) -> dict[str, str]:
        """用于幂等比较的规范化载荷。

        ``scanned_at`` 统一归一化为带时区的 UTC 表示，这样
        ``+08:00`` 与其等价的 ``Z`` 时刻被视为同一载荷；
        但归属顺序从不使用它，只由事务提交先后决定。
        """
        ts = self.scanned_at
        if ts.tzinfo is None or ts.utcoffset() is None:  # pragma: no cover - 由校验器拦截
            raise ValueError("scanned_at must be timezone-aware")
        utc = ts.astimezone(timezone.utc)
        return {
            "event_id": self.event_id,
            "band_id": self.band_id,
            "gate_id": self.gate_id,
            "scanned_at": utc.isoformat(),
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

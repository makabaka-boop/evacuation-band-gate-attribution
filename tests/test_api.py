"""API 层功能测试（FastAPI TestClient，直连真实 PostgreSQL）。

在容器/CI 中随 verify 一并运行；若数据库不可达则整体跳过，
核心的并发保证由 tests/acceptance 覆盖。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import engine


@pytest.fixture(scope="session")
def client():
    from fastapi.testclient import TestClient

    from app.main import app

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:  # pragma: no cover
        pytest.skip(f"PostgreSQL 不可达：{exc}")

    with TestClient(app) as c:
        yield c


@pytest.fixture
def ns() -> str:
    return uuid.uuid4().hex


def _body(ns: str, *, event: str, gate: str, offset: int = 0) -> dict:
    ts = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=offset)
    return {
        "event_id": f"evt-{ns}-{event}",
        "band_id": f"band-{ns}",
        "gate_id": gate,
        "scanned_at": ts.isoformat(),
    }


def test_health(client) -> None:
    assert client.get("/health").json() == {"status": "ok"}


def test_first_then_already_share_attribution(client, ns: str) -> None:
    first = client.post("/scans", json=_body(ns, event="a", gate="G1"))
    assert first.status_code == 200
    assert first.json()["result"] == "first_seen"

    second = client.post("/scans", json=_body(ns, event="b", gate="G2"))
    assert second.status_code == 200
    body = second.json()
    assert body["result"] == "already_seen"
    assert body["first_gate_id"] == "G1"
    assert body["first_seen_at"] == first.json()["first_seen_at"]


def test_query_band_fact(client, ns: str) -> None:
    created = client.post("/scans", json=_body(ns, event="q", gate="GQ")).json()
    got = client.get(f"/bands/band-{ns}")
    assert got.status_code == 200
    assert got.json()["gate_id"] == "GQ"
    assert got.json()["event_id"] == created["event_id"]


def test_query_unknown_band_404(client, ns: str) -> None:
    assert client.get(f"/bands/nope-{ns}").status_code == 404


def test_idempotent_replay_identical(client, ns: str) -> None:
    payload = _body(ns, event="same", gate="G1")
    first = client.post("/scans", json=payload)
    replay = client.post("/scans", json=payload)
    assert replay.status_code == 200
    assert replay.json() == first.json()

    # 等价时区写法（同一时刻）也视作相同载荷。
    equivalent = dict(payload)
    ts = datetime.fromisoformat(payload["scanned_at"]).astimezone(
        timezone(timedelta(hours=8))
    )
    equivalent["scanned_at"] = ts.isoformat()
    same_moment = client.post("/scans", json=equivalent)
    assert same_moment.status_code == 200
    assert same_moment.json() == first.json()


def test_same_event_different_payload_409(client, ns: str) -> None:
    client.post("/scans", json=_body(ns, event="dup", gate="G1"))
    conflict = client.post("/scans", json=_body(ns, event="dup", gate="G2"))
    assert conflict.status_code == 409
    assert "original_payload" in conflict.json()


def test_timezone_required(client, ns: str) -> None:
    payload = _body(ns, event="naive", gate="G1")
    payload["scanned_at"] = "2026-09-14T10:00:00"
    resp = client.post("/scans", json=payload)
    assert resp.status_code == 422


def test_unknown_field_rejected(client, ns: str) -> None:
    payload = _body(ns, event="extra", gate="G1")
    payload["bogus"] = 1
    assert client.post("/scans", json=payload).status_code == 422

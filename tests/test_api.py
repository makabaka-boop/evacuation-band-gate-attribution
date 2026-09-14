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


def test_different_timezone_spelling_same_event_is_409(client, ns: str) -> None:
    """同一事件换时区写法（时刻等价）也是不同载荷：拒绝且归属不变。"""
    payload = _body(ns, event="same", gate="G1")  # 基准：带 +00:00
    first = client.post("/scans", json=payload)
    assert first.status_code == 200
    assert first.json()["result"] == "first_seen"

    # 完全相同的载荷重放 -> 原响应。
    replay = client.post("/scans", json=payload)
    assert replay.status_code == 200
    assert replay.json() == first.json()

    # 同一 event_id，scanned_at 改写为等价的 +08:00 时刻 -> 409。
    equivalent = dict(payload)
    ts = datetime.fromisoformat(payload["scanned_at"]).astimezone(
        timezone(timedelta(hours=8))
    )
    equivalent["scanned_at"] = ts.isoformat()
    conflict = client.post("/scans", json=equivalent)
    assert conflict.status_code == 409
    assert "original_payload" in conflict.json()

    # 归属仍是原闸机；409 没有新增任何记录。
    got = client.get(f"/bands/band-{ns}")
    assert got.status_code == 200
    assert got.json()["gate_id"] == "G1"


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


# ---------------------------------------------------------------------------
# 疏散名册：创建与核对
# ---------------------------------------------------------------------------


def _roster(
    ns: str,
    *,
    roster: str = "main",
    name: str = "3F 东侧疏散名册",
    bands: list[str] | None = None,
) -> dict:
    band_list = bands if bands is not None else [f"band-{ns}-{i}" for i in range(3)]
    return {
        "roster_id": f"roster-{ns}-{roster}",
        "name": name,
        "band_ids": band_list,
    }


def _scan(client, ns: str, *, event: str, band: str, gate: str = "G1"):
    return client.post(
        "/scans",
        json={
            "event_id": f"evt-{ns}-{event}",
            "band_id": band,
            "gate_id": gate,
            "scanned_at": datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc).isoformat(),
        },
    )


def _roster_row_counts(roster_id: str) -> tuple[int, int]:
    with engine.connect() as conn:
        rosters = conn.execute(
            text("SELECT COUNT(*) FROM evacuation_rosters WHERE roster_id = :r"),
            {"r": roster_id},
        ).scalar_one()
        members = conn.execute(
            text("SELECT COUNT(*) FROM roster_members WHERE roster_id = :r"),
            {"r": roster_id},
        ).scalar_one()
    return rosters, members


def test_roster_create_then_partial_check(client, ns: str) -> None:
    body = _roster(ns)
    created = client.post("/rosters", json=body)
    assert created.status_code == 201
    assert created.json() == {
        "roster_id": body["roster_id"],
        "name": body["name"],
        "expected_count": 3,
    }

    # 两名成员过闸（其中一人被两台闸机扫到，仍只算一人）。
    assert _scan(client, ns, event="s1", band=f"band-{ns}-0").status_code == 200
    assert _scan(client, ns, event="s2", band=f"band-{ns}-2").status_code == 200
    assert _scan(client, ns, event="s3", band=f"band-{ns}-2", gate="G2").status_code == 200
    # 非名册成员的扫描不影响核对。
    assert _scan(client, ns, event="s4", band=f"band-{ns}-outsider").status_code == 200

    got = client.get(f"/rosters/{body['roster_id']}")
    assert got.status_code == 200
    data = got.json()
    assert data["roster_id"] == body["roster_id"]
    assert data["name"] == body["name"]
    assert data["expected_count"] == 3
    assert data["passed_count"] == 2
    assert data["missing_count"] == 1
    assert data["missing_band_ids"] == [f"band-{ns}-1"]


def test_roster_missing_sorted_by_band_id(client, ns: str) -> None:
    bands = [f"band-{ns}-z", f"band-{ns}-m", f"band-{ns}-a"]
    body = _roster(ns, bands=bands)
    assert client.post("/rosters", json=body).status_code == 201

    data = client.get(f"/rosters/{body['roster_id']}").json()
    assert data["missing_band_ids"] == sorted(bands)
    assert data["missing_count"] == 3
    assert data["passed_count"] == 0


def test_roster_all_passed_missing_zero(client, ns: str) -> None:
    body = _roster(ns)
    assert client.post("/rosters", json=body).status_code == 201
    for i, band in enumerate(body["band_ids"]):
        assert _scan(client, ns, event=f"all{i}", band=band).status_code == 200

    data = client.get(f"/rosters/{body['roster_id']}").json()
    assert data["expected_count"] == 3
    assert data["passed_count"] == 3
    assert data["missing_count"] == 0
    assert data["missing_band_ids"] == []


def test_roster_scans_affect_only_later_checks(client, ns: str) -> None:
    """核对是即时的：每次查询反映其快照时刻，之后的过闸只影响后续查询。"""
    body = _roster(ns, bands=[f"band-{ns}-a", f"band-{ns}-b"])
    assert client.post("/rosters", json=body).status_code == 201

    first = client.get(f"/rosters/{body['roster_id']}").json()
    assert first["missing_count"] == 2

    _scan(client, ns, event="later1", band=f"band-{ns}-a")
    second = client.get(f"/rosters/{body['roster_id']}").json()
    assert second["missing_count"] == 1
    assert second["missing_band_ids"] == [f"band-{ns}-b"]

    _scan(client, ns, event="later2", band=f"band-{ns}-b")
    third = client.get(f"/rosters/{body['roster_id']}").json()
    assert third["missing_count"] == 0


def test_roster_empty_band_ids_422_and_no_rows(client, ns: str) -> None:
    body = _roster(ns, roster="empty", bands=[])
    assert client.post("/rosters", json=body).status_code == 422
    assert _roster_row_counts(body["roster_id"]) == (0, 0)


def test_roster_duplicate_band_ids_422_and_no_rows(client, ns: str) -> None:
    body = _roster(ns, roster="dup", bands=[f"band-{ns}-a", f"band-{ns}-a"])
    assert client.post("/rosters", json=body).status_code == 422
    assert _roster_row_counts(body["roster_id"]) == (0, 0)


def test_roster_blank_fields_422(client, ns: str) -> None:
    body = _roster(ns, roster="blank")
    assert client.post("/rosters", json={**body, "roster_id": ""}).status_code == 422
    assert client.post("/rosters", json={**body, "name": ""}).status_code == 422
    assert client.post("/rosters", json={**body, "band_ids": [""]}).status_code == 422
    assert client.post("/rosters", json={**body, "bogus": 1}).status_code == 422
    assert _roster_row_counts(body["roster_id"]) == (0, 0)


def test_roster_whitespace_only_fields_422_and_no_rows(client, ns: str) -> None:
    """纯空白（空格/制表/换行）标识、名称、腕带号一律在持久化前拒绝。"""
    body = _roster(ns, roster="whitespace")

    # 纯空白 roster_id：不可辨识的标识不得创建出名册。
    assert client.post("/rosters", json={**body, "roster_id": "   "}).status_code == 422
    # 纯空白 name：不保留不可辨识的名称。
    assert client.post("/rosters", json={**body, "name": "\t\n  "}).status_code == 422
    # 纯空白腕带号：不得计入应到/未通过人数。
    assert client.post(
        "/rosters", json={**body, "band_ids": ["   "]}
    ).status_code == 422
    assert client.post(
        "/rosters", json={**body, "band_ids": [f"band-{ns}-ok", "\t"]}
    ).status_code == 422

    # 全部在 Pydantic 层拒绝，名册表/成员表都不留任何残行。
    assert _roster_row_counts(body["roster_id"]) == (0, 0)


def test_roster_id_with_slash_is_addressable_after_creation(client, ns: str) -> None:
    """含斜杠的 roster_id 创建后必须仍可按原标识核对（原始/编码斜杠均可）。"""
    roster_id = f"team-{ns}/3f-east"
    body = {
        "roster_id": roster_id,
        "name": "3F 东侧车间",
        "band_ids": [f"band-{ns}-0", f"band-{ns}-1"],
    }

    created = client.post("/rosters", json=body)
    assert created.status_code == 201
    assert created.json()["roster_id"] == roster_id

    # 原始斜杠逐字寻址。
    got = client.get(f"/rosters/{roster_id}")
    assert got.status_code == 200
    assert got.json()["roster_id"] == roster_id
    assert got.json()["expected_count"] == 2
    assert got.json()["missing_band_ids"] == [f"band-{ns}-0", f"band-{ns}-1"]

    # 一名成员过闸后，差分经同一标识仍正确。
    assert _scan(client, ns, event="sl1", band=f"band-{ns}-0").status_code == 200
    got = client.get(f"/rosters/{roster_id}")
    assert got.status_code == 200
    assert got.json()["passed_count"] == 1
    assert got.json()["missing_band_ids"] == [f"band-{ns}-1"]

    # URL 编码的 %2F 必须解码回同一 roster_id，命中同一名册。
    encoded = roster_id.replace("/", "%2F")
    got_encoded = client.get(f"/rosters/{encoded}")
    assert got_encoded.status_code == 200
    assert got_encoded.json()["roster_id"] == roster_id
    assert got_encoded.json() == got.json()


def test_roster_duplicate_id_409_and_original_preserved(client, ns: str) -> None:
    body = _roster(ns, name="原名册", bands=[f"band-{ns}-a", f"band-{ns}-b"])
    assert client.post("/rosters", json=body).status_code == 201

    conflict = client.post(
        "/rosters",
        json={**body, "name": "冒名名册", "band_ids": [f"band-{ns}-x"]},
    )
    assert conflict.status_code == 409

    # 原名册分毫不动：名称、成员、核对结果均保持首次创建的内容。
    data = client.get(f"/rosters/{body['roster_id']}").json()
    assert data["name"] == "原名册"
    assert data["expected_count"] == 2
    assert data["missing_band_ids"] == [f"band-{ns}-a", f"band-{ns}-b"]
    assert _roster_row_counts(body["roster_id"]) == (1, 2)


def test_roster_unknown_404(client, ns: str) -> None:
    assert client.get(f"/rosters/nope-{ns}").status_code == 404

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


# ---------------------------------------------------------------------------
# 闸机巡检：提交、最近一次查询与可用/故障状态
# ---------------------------------------------------------------------------


def _insp(
    ns: str,
    *,
    insp: str = "i1",
    gate: str | None = None,
    offset: int = 0,
    conclusion: str = "available",
    notes: str | None = None,
) -> dict:
    ts = datetime(2026, 9, 14, 8, 0, tzinfo=timezone.utc) + timedelta(minutes=offset)
    body = {
        "inspection_id": f"insp-{ns}-{insp}",
        "gate_id": gate if gate is not None else f"GATE-{ns}",
        "checked_at": ts.isoformat(),
        "conclusion": conclusion,
    }
    if notes is not None:
        body["notes"] = notes
    return body


def _inspection_counts(*, gate: str, id_prefix: str) -> tuple[int, int]:
    with engine.connect() as conn:
        by_gate = conn.execute(
            text("SELECT COUNT(*) FROM gate_inspections WHERE gate_id = :g"),
            {"g": gate},
        ).scalar_one()
        by_id = conn.execute(
            text("SELECT COUNT(*) FROM gate_inspections WHERE inspection_id LIKE :p"),
            {"p": id_prefix},
        ).scalar_one()
    return by_gate, by_id


def test_inspection_submit_then_latest_query(client, ns: str) -> None:
    body = _insp(ns, insp="first", conclusion="available", notes="例行巡检")
    created = client.post("/inspections", json=body)
    assert created.status_code == 201
    data = created.json()
    assert data["inspection_id"] == body["inspection_id"]
    assert data["gate_id"] == body["gate_id"]
    assert data["conclusion"] == "available"
    assert data["notes"] == "例行巡检"
    assert data["seq"] > 0
    assert "recorded_at" in data

    got = client.get(f"/gates/GATE-{ns}/inspections/latest")
    assert got.status_code == 200
    latest = got.json()
    assert latest["gate_id"] == f"GATE-{ns}"
    assert latest["status"] == "available"
    assert latest["latest_inspection"] == data


def test_inspection_notes_optional(client, ns: str) -> None:
    body = _insp(ns, insp="no-notes")
    assert client.post("/inspections", json=body).status_code == 201
    latest = client.get(f"/gates/GATE-{ns}/inspections/latest").json()
    assert latest["latest_inspection"]["notes"] is None


def test_inspection_latest_follows_submission_order_not_checked_at(
    client, ns: str
) -> None:
    """checked_at 乱序时，“最近一次”仍按提交顺序（数据库递增序号）裁决。"""
    # 提交顺序与客户端时钟大小交错：声称的时刻分别为 +50 / -20 / +10。
    first = client.post("/inspections", json=_insp(ns, insp="t1", offset=50))
    second = client.post(
        "/inspections", json=_insp(ns, insp="t2", offset=-20, conclusion="faulty")
    )
    third = client.post(
        "/inspections",
        json=_insp(ns, insp="t3", offset=10, conclusion="available", notes="复核通过"),
    )
    assert [r.status_code for r in (first, second, third)] == [201, 201, 201]

    # 数据库生成的序号随提交顺序递增。
    seqs = [r.json()["seq"] for r in (first, second, third)]
    assert seqs == sorted(seqs)

    latest = client.get(f"/gates/GATE-{ns}/inspections/latest").json()
    # checked_at 最大的是第一条（+50），但最新记录是最后提交的第三条。
    record = latest["latest_inspection"]
    assert record["inspection_id"] == f"insp-{ns}-t3"
    assert record["seq"] == seqs[2]
    assert record["notes"] == "复核通过"
    assert latest["status"] == "available"


def test_inspection_duplicate_id_409_and_original_preserved(client, ns: str) -> None:
    original = _insp(ns, insp="dup", conclusion="available", notes="原始记录")
    created = client.post("/inspections", json=original)
    assert created.status_code == 201

    conflict = client.post(
        "/inspections",
        json=_insp(ns, insp="dup", offset=99, conclusion="faulty", notes="覆盖尝试"),
    )
    assert conflict.status_code == 409
    assert conflict.json()["inspection_id"] == original["inspection_id"]

    # 原记录分毫不动：结论、备注、检查时间均为首次提交的内容。
    latest = client.get(f"/gates/GATE-{ns}/inspections/latest").json()
    record = latest["latest_inspection"]
    assert record["conclusion"] == "available"
    assert record["notes"] == "原始记录"
    assert record["checked_at"] == created.json()["checked_at"]
    assert latest["status"] == "available"
    # 重复提交没有新增任何行。
    assert _inspection_counts(gate=f"GATE-{ns}", id_prefix=f"insp-{ns}-%") == (1, 1)


def test_inspection_invalid_submissions_422_and_no_rows(client, ns: str) -> None:
    """空白标识/闸机号、无时区时间、超长备注等一律 422，且不留残行。"""
    gate = f"GATE-{ns}-invalid"
    base = _insp(ns, insp="bad", gate=gate)
    cases = [
        {**base, "inspection_id": ""},
        {**base, "inspection_id": "   "},
        {**base, "inspection_id": " \t\n "},
        {**base, "gate_id": ""},
        {**base, "gate_id": "  "},
        {**base, "checked_at": "2026-09-14T08:00:00"},  # 无时区偏移
        {**base, "notes": "x" * 501},  # 超长备注
        {**base, "conclusion": "maybe"},  # 非法结论
        {**base, "bogus": 1},  # 未知字段
    ]
    for body in cases:
        assert client.post("/inspections", json=body).status_code == 422, body

    # 全部在 Pydantic 层拒绝，巡检表不留任何残行。
    assert _inspection_counts(gate=gate, id_prefix=f"insp-{ns}-%") == (0, 0)


def test_inspection_notes_max_length_boundary(client, ns: str) -> None:
    ok = client.post("/inspections", json=_insp(ns, insp="max", notes="备" * 500))
    assert ok.status_code == 201
    too_long = client.post("/inspections", json=_insp(ns, insp="over", notes="备" * 501))
    assert too_long.status_code == 422


def test_inspection_unknown_gate_404(client, ns: str) -> None:
    resp = client.get(f"/gates/GATE-{ns}-nope/inspections/latest")
    assert resp.status_code == 404


def test_faulty_conclusion_does_not_block_scans(client, ns: str) -> None:
    """故障结论只影响状态查询：既有扫描归属与名册核对完全不受影响。"""
    gate = f"GATE-{ns}"
    assert (
        client.post(
            "/inspections", json=_insp(ns, insp="faulty", gate=gate, conclusion="faulty")
        ).status_code
        == 201
    )
    assert client.get(f"/gates/{gate}/inspections/latest").json()["status"] == "faulty"

    # 故障闸机上的扫描仍正常裁决归属。
    first = _scan(client, ns, event="f1", band=f"band-{ns}-0", gate=gate)
    assert first.status_code == 200
    assert first.json()["result"] == "first_seen"
    assert first.json()["first_gate_id"] == gate

    second = _scan(client, ns, event="f2", band=f"band-{ns}-0", gate="GATE-OTHER")
    assert second.json()["result"] == "already_seen"
    assert second.json()["first_gate_id"] == gate

    # 名册核对照常复用首次通过事实。
    body = _roster(ns, roster="faulty-gate", bands=[f"band-{ns}-0", f"band-{ns}-1"])
    assert client.post("/rosters", json=body).status_code == 201
    data = client.get(f"/rosters/{body['roster_id']}").json()
    assert data["passed_count"] == 1
    assert data["missing_band_ids"] == [f"band-{ns}-1"]

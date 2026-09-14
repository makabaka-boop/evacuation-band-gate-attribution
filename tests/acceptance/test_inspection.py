"""闸机巡检的一次性验收：真实 PostgreSQL、独立进程、独立连接。

覆盖：
* 检查时间乱序提交时，“最近一次”仍按提交顺序（数据库递增序号）裁决；
* 并发提交同一 inspection_id 恰有一个 201，其余 409，原记录保留；
* 同一闸机的并发不同巡检全部追加落库，最新记录取最大 seq；
* 故障结论不阻断既有扫描的并发归属与名册核对；
* 非法提交（空白标识/闸机号、无时区时间、超长备注）422 且不留残行；
* 无记录闸机查询 404；HTTP 端到端全流程。

运行方式与 test_evacuation.py 相同（由 verify 服务执行）。
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

from .conftest import (
    DATABASE_URL,
    barrier_submit_concurrently,
    iso,
    result_sanity,
    run_in_process,
    submit_inspection_concurrently,
)

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


@pytest.fixture
def ns() -> str:
    return uuid.uuid4().hex


def _insp_payload(
    ns: str,
    *,
    insp: str,
    gate: str,
    offset_minutes: int = 0,
    conclusion: str = "available",
    notes: str | None = None,
) -> dict:
    ts = datetime(2026, 9, 14, 8, 0, 0, tzinfo=timezone.utc) + timedelta(
        minutes=offset_minutes
    )
    body = {
        "inspection_id": f"insp-{ns}-{insp}",
        "gate_id": gate,
        "checked_at": iso(ts),
        "conclusion": conclusion,
    }
    if notes is not None:
        body["notes"] = notes
    return body


def _scan_payload(ns: str, *, event: str, band: str, gate: str) -> dict:
    return {
        "event_id": f"evt-{ns}-{event}",
        "band_id": band,
        "gate_id": gate,
        "scanned_at": iso(datetime(2026, 9, 14, 10, 0, 0, tzinfo=timezone.utc)),
    }


def _inspection_rows(where: str, params: dict) -> list[tuple]:
    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            return list(
                conn.execute(
                    text(
                        "SELECT inspection_id, gate_id, conclusion, seq "
                        f"FROM gate_inspections WHERE {where}"
                    ),
                    params,
                ).all()
            )
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# 1) 检查时间乱序：最新记录仍按提交顺序（数据库递增序号）裁决
# ---------------------------------------------------------------------------


def test_latest_follows_submission_order_with_scrambled_clocks(ns: str) -> None:
    gate = f"GATE-{ns}"
    # 提交顺序与客户端时钟交错：声称的时刻分别为 +50 / -20 / +10 分钟。
    submissions = [
        _insp_payload(ns, insp="t1", gate=gate, offset_minutes=50),
        _insp_payload(ns, insp="t2", gate=gate, offset_minutes=-20, conclusion="faulty"),
        _insp_payload(
            ns, insp="t3", gate=gate, offset_minutes=10, notes="复核通过"
        ),
    ]

    seqs: list[int] = []
    for payload in submissions:
        result = run_in_process("_worker_submit_inspection", payload)
        result_sanity(result)
        assert result["status"] == 201
        seqs.append(result["body"]["seq"])

    # 数据库生成的序号随提交顺序严格递增。
    assert seqs == sorted(seqs)

    latest = run_in_process("_worker_latest_inspection", gate)
    result_sanity(latest)
    assert latest["status"] == 200
    body = latest["body"]
    record = body["latest_inspection"]
    # checked_at 最大的是第一条（+50），但最新记录是最后提交的第三条。
    assert record["inspection_id"] == submissions[2]["inspection_id"]
    assert record["seq"] == seqs[2]
    assert record["notes"] == "复核通过"
    assert body["status"] == "available"
    assert body["gate_id"] == gate

    # 与表内最大 seq 一致 —— 最新裁决只认数据库序号。
    rows = _inspection_rows("gate_id = :g", {"g": gate})
    assert len(rows) == 3
    assert record["seq"] == max(row.seq for row in rows)


# ---------------------------------------------------------------------------
# 2) 并发提交同一 inspection_id：恰有一个 201，其余 409，原记录保留
# ---------------------------------------------------------------------------


def test_concurrent_same_inspection_id_single_winner(ns: str) -> None:
    gate = f"GATE-{ns}"
    base = _insp_payload(ns, insp="hot", gate=gate)
    clones = [
        {
            **base,
            "conclusion": conclusion,
            "notes": f"并发-{i}",
            "_racers": 3,
            "_slot": f"slot-{i}",
        }
        for i, conclusion in enumerate(["available", "faulty", "available"])
    ]
    results = submit_inspection_concurrently(f"bar-insp-{ns}", clones)

    assert len(results) == 3
    for result in results:
        result_sanity(result)
    statuses = sorted(r["status"] for r in results)
    assert statuses == [201, 409, 409], results

    # 只有胜者的内容落库；落败的 409 不改写任何数据。
    winner = next(r for r in results if r["status"] == 201)["body"]
    latest = run_in_process("_worker_latest_inspection", gate)
    result_sanity(latest)
    assert latest["body"]["latest_inspection"] == winner
    assert latest["body"]["status"] == winner["conclusion"]

    rows = _inspection_rows("inspection_id = :i", {"i": base["inspection_id"]})
    assert len(rows) == 1
    assert rows[0].seq == winner["seq"]


# ---------------------------------------------------------------------------
# 3) 同一闸机的并发不同巡检：全部追加落库，最新记录取最大 seq
# ---------------------------------------------------------------------------


def test_concurrent_inspections_same_gate_all_appended(ns: str) -> None:
    gate = f"GATE-{ns}"
    payloads = [
        {
            **_insp_payload(ns, insp=f"c{i}", gate=gate, offset_minutes=i * 7 - 10),
            "_racers": 4,
            "_slot": f"slot-{i}",
        }
        for i in range(4)
    ]
    results = submit_inspection_concurrently(f"bar-insp-many-{ns}", payloads)

    assert len(results) == 4
    for result in results:
        result_sanity(result)
        assert result["status"] == 201

    # 追加式持久化：四条记录全部落库，无一被覆盖。
    rows = _inspection_rows("gate_id = :g", {"g": gate})
    assert len(rows) == 4
    assert {row.inspection_id for row in rows} == {
        p["inspection_id"] for p in payloads
    }

    # 最新记录即 seq 最大者（提交顺序），与各客户端时钟无关。
    newest = max(rows, key=lambda row: row.seq)
    latest = run_in_process("_worker_latest_inspection", gate)
    result_sanity(latest)
    record = latest["body"]["latest_inspection"]
    assert record["seq"] == newest.seq
    assert record["inspection_id"] == newest.inspection_id


# ---------------------------------------------------------------------------
# 4) 故障结论不阻断既有扫描：并发归属与名册核对照常
# ---------------------------------------------------------------------------


def test_faulty_conclusion_does_not_block_scan_attribution(ns: str) -> None:
    gate = f"GATE-{ns}"

    # 闸机被标记故障，状态查询如实反映。
    faulty = run_in_process(
        "_worker_submit_inspection",
        _insp_payload(ns, insp="faulty", gate=gate, conclusion="faulty", notes="门体异响"),
    )
    result_sanity(faulty)
    assert faulty["status"] == 201
    latest = run_in_process("_worker_latest_inspection", gate)
    result_sanity(latest)
    assert latest["body"]["status"] == "faulty"

    # 既有扫描不受影响：四个独立进程在故障闸机上竞争同一腕带，
    # 仍恰有一个 first_seen，归属事实一致。
    band = f"band-{ns}"
    payloads = [
        {
            **_scan_payload(ns, event=f"g{i}", band=band, gate=gate),
            "_racers": 4,
        }
        for i in range(4)
    ]
    results = barrier_submit_concurrently(f"bar-scan-{ns}", payloads)
    assert len(results) == 4
    for result in results:
        result_sanity(result)
    bodies = [r["body"] for r in results]
    firsts = [b for b in bodies if b["result"] == "first_seen"]
    assert len(firsts) == 1
    for body in bodies:
        assert body["first_gate_id"] == gate
        assert body["first_seen_at"] == firsts[0]["first_seen_at"]

    # 名册核对照常复用该首次通过事实。
    roster_id = f"roster-{ns}"
    created = run_in_process(
        "_worker_create_roster",
        {"roster_id": roster_id, "name": "故障闸机演练名册", "band_ids": [band]},
    )
    result_sanity(created)
    assert created["status"] == 201
    checked = run_in_process("_worker_check_roster", roster_id)
    result_sanity(checked)
    assert checked["body"]["passed_count"] == 1
    assert checked["body"]["missing_count"] == 0


# ---------------------------------------------------------------------------
# 5) 非法提交 422 且不留残行；无记录闸机 404
# ---------------------------------------------------------------------------


def test_invalid_submissions_422_and_leave_no_rows(ns: str) -> None:
    httpx = pytest.importorskip("httpx")

    gate = f"GATE-{ns}-invalid"
    base = _insp_payload(ns, insp="bad", gate=gate)
    cases = [
        {**base, "inspection_id": ""},
        {**base, "inspection_id": "   "},
        {**base, "gate_id": ""},
        {**base, "gate_id": " \t\n "},
        {**base, "checked_at": "2026-09-14T08:00:00"},  # 无时区偏移
        {**base, "notes": "x" * 501},  # 超长备注
        {**base, "conclusion": "unknown"},  # 非法结论
        {**base, "bogus": 1},  # 未知字段
    ]

    try:
        with httpx.Client(base_url=API_BASE_URL, timeout=30) as client:
            codes = [
                client.post("/inspections", json=body).status_code for body in cases
            ]
            unknown = client.get(f"/gates/{gate}/inspections/latest")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"API 不可达（{API_BASE_URL}）：{exc}")

    assert codes == [422] * len(cases)
    assert unknown.status_code == 404

    # 非法提交不得在巡检表留下任何残行。
    assert _inspection_rows("gate_id = :g", {"g": gate}) == []
    assert _inspection_rows("inspection_id LIKE :p", {"p": f"insp-{ns}-%"}) == []

    # 服务层查询无记录闸机同样为“不存在”。
    result = run_in_process("_worker_latest_inspection", gate)
    result_sanity(result)
    assert result["status"] == 404


# ---------------------------------------------------------------------------
# 6) HTTP 端到端：提交 -> 最新查询 -> 重复 409 -> 未知闸机 404 -> 故障不阻断扫描
# ---------------------------------------------------------------------------


def test_http_inspection_flow_against_live_api(ns: str) -> None:
    """跨进程经真实 HTTP -> 双 uvicorn worker -> PostgreSQL 的巡检全流程。"""
    httpx = pytest.importorskip("httpx")

    gate = f"GATE-{ns}"
    first = _insp_payload(ns, insp="h1", gate=gate, offset_minutes=20)
    # 时钟更早但后提交：最新记录必须是它。
    second = _insp_payload(
        ns, insp="h2", gate=gate, offset_minutes=5, conclusion="faulty", notes="门体异响"
    )
    scan = _scan_payload(ns, event="h1", band=f"band-{ns}", gate=gate)

    try:
        with httpx.Client(base_url=API_BASE_URL, timeout=30) as client:
            r1 = client.post("/inspections", json=first)
            r2 = client.post("/inspections", json=second)
            latest = client.get(f"/gates/{gate}/inspections/latest")
            duplicate = client.post("/inspections", json={**first, "conclusion": "faulty"})
            unknown = client.get(f"/gates/GATE-{ns}-unknown/inspections/latest")
            after_duplicate = client.get(f"/gates/{gate}/inspections/latest")
            scan_resp = client.post("/scans", json=scan)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"API 不可达（{API_BASE_URL}）：{exc}")

    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r2.json()["seq"] > r1.json()["seq"]

    assert latest.status_code == 200
    body = latest.json()
    assert body["gate_id"] == gate
    assert body["status"] == "faulty"
    assert body["latest_inspection"]["inspection_id"] == second["inspection_id"]
    assert body["latest_inspection"]["notes"] == "门体异响"
    assert body["latest_inspection"]["seq"] == r2.json()["seq"]

    # 重复 inspection_id -> 409，且不覆盖原记录、不新增行。
    assert duplicate.status_code == 409
    assert after_duplicate.json() == body
    rows = _inspection_rows("gate_id = :g", {"g": gate})
    assert len(rows) == 2

    assert unknown.status_code == 404

    # 故障结论不阻断既有扫描：同一闸机照常完成首次归属。
    assert scan_resp.status_code == 200
    assert scan_resp.json()["result"] == "first_seen"
    assert scan_resp.json()["first_gate_id"] == gate

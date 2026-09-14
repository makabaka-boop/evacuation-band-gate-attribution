"""疏散名册核对的一次性验收：真实 PostgreSQL、独立进程、独立连接。

覆盖：
* 部分成员过闸时，集合差分（未通过名单）精确正确；
* 全部过闸后未通过数归零；
* 核对期间并发提交的扫描只产生一致的快照，且只影响后续查询；
* 非法创建（空名单/重复腕带）返回 422 且不留残行；
* 重复 roster_id 并发创建恰有一个 201，其余 409，原名册保留；
* 未知名册查询返回 404；
* HTTP 端到端（双 worker 的真实 API）。

运行方式与 test_evacuation.py 相同（由 verify 服务执行）。
"""
from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

from app.database import SessionLocal
from app.service import check_roster

from .conftest import (
    DATABASE_URL,
    create_roster_concurrently,
    iso,
    result_sanity,
    run_in_process,
    submit_concurrently,
)

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


@pytest.fixture
def ns() -> str:
    return uuid.uuid4().hex


def _roster_payload(ns: str, *, roster: str = "main", name: str = "疏散名册", count: int = 5) -> dict:
    return {
        "roster_id": f"roster-{ns}-{roster}",
        "name": name,
        "band_ids": [f"band-{ns}-{i}" for i in range(count)],
    }


def _scan_payload(ns: str, *, event: str, band: str, gate: str, offset_minutes: int = 0) -> dict:
    ts = datetime(2026, 9, 14, 10, 0, 0, tzinfo=timezone.utc) + timedelta(
        minutes=offset_minutes
    )
    return {
        "event_id": f"evt-{ns}-{event}",
        "band_id": band,
        "gate_id": gate,
        "scanned_at": iso(ts),
    }


def _create(payload: dict) -> dict:
    result = run_in_process("_worker_create_roster", payload)
    result_sanity(result)
    return result


def _check(roster_id: str) -> dict:
    result = run_in_process("_worker_check_roster", roster_id)
    result_sanity(result)
    return result


def _table_counts(roster_id: str) -> tuple[int, int]:
    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            rosters = conn.execute(
                text("SELECT COUNT(*) FROM evacuation_rosters WHERE roster_id = :r"),
                {"r": roster_id},
            ).scalar_one()
            members = conn.execute(
                text("SELECT COUNT(*) FROM roster_members WHERE roster_id = :r"),
                {"r": roster_id},
            ).scalar_one()
    finally:
        engine.dispose()
    return rosters, members


# ---------------------------------------------------------------------------
# 1) 部分成员过闸：集合差分精确正确
# ---------------------------------------------------------------------------


def test_partial_pass_set_difference_is_exact(ns: str) -> None:
    roster = _roster_payload(ns, count=5)
    created = _create(roster)
    assert created["status"] == 201
    assert created["body"]["expected_count"] == 5

    # 前 3 名成员在各自独立进程中并发过闸。
    passed = roster["band_ids"][:3]
    results = submit_concurrently(
        [
            _scan_payload(ns, event=f"p{i}", band=band, gate=f"GATE-{i}")
            for i, band in enumerate(passed)
        ]
    )
    assert len(results) == 3
    for result in results:
        result_sanity(result)
        assert result["body"]["result"] == "first_seen"

    # 非名册成员的过闸不得影响差分。
    outsider = run_in_process(
        "_worker_submit",
        _scan_payload(ns, event="outsider", band=f"band-{ns}-outsider", gate="GATE-9"),
    )
    result_sanity(outsider)

    checked = _check(roster["roster_id"])
    assert checked["status"] == 200
    body = checked["body"]
    assert body["expected_count"] == 5
    assert body["passed_count"] == 3
    assert body["missing_count"] == 2
    # 差分恰好是未过闸的两名成员，且按腕带编号稳定排序。
    assert body["missing_band_ids"] == roster["band_ids"][3:]
    assert body["missing_band_ids"] == sorted(body["missing_band_ids"])


# ---------------------------------------------------------------------------
# 2) 全部过闸后未通过归零
# ---------------------------------------------------------------------------


def test_all_passed_missing_goes_to_zero(ns: str) -> None:
    roster = _roster_payload(ns, count=4)
    assert _create(roster)["status"] == 201

    results = submit_concurrently(
        [
            _scan_payload(ns, event=f"a{i}", band=band, gate=f"GATE-{i}")
            for i, band in enumerate(roster["band_ids"])
        ]
    )
    for result in results:
        result_sanity(result)

    body = _check(roster["roster_id"])["body"]
    assert body["expected_count"] == 4
    assert body["passed_count"] == 4
    assert body["missing_count"] == 0
    assert body["missing_band_ids"] == []


# ---------------------------------------------------------------------------
# 3) 核对期间并发过闸：每次查询都是内部一致的快照，只影响后续请求
# ---------------------------------------------------------------------------


def test_checks_stay_consistent_while_scans_commit(ns: str) -> None:
    roster = _roster_payload(ns, count=6)
    assert _create(roster)["status"] == 201
    bands = set(roster["band_ids"])

    payloads = [
        _scan_payload(ns, event=f"c{i}", band=band, gate=f"GATE-{i}")
        for i, band in enumerate(roster["band_ids"])
    ]

    # 后台线程驱动 6 个独立进程并发过闸；父进程同时反复核对。
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(submit_concurrently, payloads)
        session = SessionLocal()
        try:
            for _ in range(30):
                result = check_roster(session, roster["roster_id"])
                session.rollback()  # 下一语句取新快照（READ COMMITTED）
                assert result is not None
                # 汇总数字与明细必然来自同一快照，内部一致。
                assert result.expected_count == 6
                assert result.passed_count + result.missing_count == 6
                assert result.missing_count == len(result.missing_band_ids)
                assert result.missing_band_ids == sorted(result.missing_band_ids)
                assert set(result.missing_band_ids) <= bands
        finally:
            session.close()
        results = future.result()

    for result in results:
        result_sanity(result)

    # 全部提交后的后续查询：未通过归零。
    final = _check(roster["roster_id"])["body"]
    assert final["missing_count"] == 0
    assert final["passed_count"] == 6


# ---------------------------------------------------------------------------
# 4) 非法创建不留残行（HTTP 422），未知名册 404
# ---------------------------------------------------------------------------


def test_invalid_creation_leaves_no_rows_and_unknown_roster_404(ns: str) -> None:
    httpx = pytest.importorskip("httpx")

    try:
        with httpx.Client(base_url=API_BASE_URL, timeout=30) as client:
            empty = client.post(
                "/rosters",
                json={
                    "roster_id": f"roster-{ns}-empty",
                    "name": "空名单",
                    "band_ids": [],
                },
            )
            duplicated = client.post(
                "/rosters",
                json={
                    "roster_id": f"roster-{ns}-dup",
                    "name": "重复腕带",
                    "band_ids": [f"band-{ns}-a", f"band-{ns}-a"],
                },
            )
            unknown = client.get(f"/rosters/roster-{ns}-unknown")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"API 不可达（{API_BASE_URL}）：{exc}")

    assert empty.status_code == 422
    assert duplicated.status_code == 422
    assert unknown.status_code == 404

    # 非法创建不得在名册表/成员表留下任何残行。
    assert _table_counts(f"roster-{ns}-empty") == (0, 0)
    assert _table_counts(f"roster-{ns}-dup") == (0, 0)

    # 服务层核对未知名册同样为“不存在”。
    assert _check(f"roster-{ns}-unknown")["status"] == 404


# ---------------------------------------------------------------------------
# 5) 并发创建同一 roster_id：恰有一个 201，其余 409，原名册保留
# ---------------------------------------------------------------------------


def test_concurrent_create_same_roster_id_single_winner(ns: str) -> None:
    base = _roster_payload(ns, count=3)
    clones = [
        {**base, "name": f"名册-{i}", "_racers": 3, "_slot": f"slot-{i}"}
        for i in range(3)
    ]
    results = create_roster_concurrently(f"bar-roster-{ns}", clones)

    assert len(results) == 3
    for result in results:
        result_sanity(result)
    statuses = sorted(r["status"] for r in results)
    assert statuses == [201, 409, 409], results

    # 只有胜者的内容落库；落败的 409 不改写任何数据。
    body = _check(base["roster_id"])["body"]
    assert body["name"] in {f"名册-{i}" for i in range(3)}
    assert body["expected_count"] == 3
    assert body["missing_band_ids"] == base["band_ids"]
    assert _table_counts(base["roster_id"]) == (1, 3)


# ---------------------------------------------------------------------------
# 6) HTTP 端到端：创建 -> 部分过闸核对 -> 全部过闸归零 -> 409/404
# ---------------------------------------------------------------------------


def test_http_roster_flow_against_live_api(ns: str) -> None:
    """跨进程经真实 HTTP -> 双 uvicorn worker -> PostgreSQL 的名册全流程。"""
    httpx = pytest.importorskip("httpx")

    roster = _roster_payload(ns, count=3)
    scans = [
        _scan_payload(ns, event=f"h{i}", band=band, gate=f"GATE-{i}")
        for i, band in enumerate(roster["band_ids"])
    ]

    try:
        with httpx.Client(base_url=API_BASE_URL, timeout=30) as client:
            created = client.post("/rosters", json=roster)
            first_scan = client.post("/scans", json=scans[0])
            partial = client.get(f"/rosters/{roster['roster_id']}")
            for payload in scans[1:]:
                client.post("/scans", json=payload)
            final = client.get(f"/rosters/{roster['roster_id']}")
            duplicate = client.post("/rosters", json={**roster, "name": "冒名名册"})
            unknown = client.get(f"/rosters/roster-{ns}-unknown")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"API 不可达（{API_BASE_URL}）：{exc}")

    assert created.status_code == 201
    assert created.json()["expected_count"] == 3
    assert first_scan.status_code == 200

    partial_body = partial.json()
    assert partial.status_code == 200
    assert partial_body["expected_count"] == 3
    assert partial_body["passed_count"] == 1
    assert partial_body["missing_count"] == 2
    assert partial_body["missing_band_ids"] == roster["band_ids"][1:]

    final_body = final.json()
    assert final.status_code == 200
    assert final_body["passed_count"] == 3
    assert final_body["missing_count"] == 0
    assert final_body["missing_band_ids"] == []

    # 重复 roster_id -> 409，且原名册未被改写。
    assert duplicate.status_code == 409
    assert unknown.status_code == 404
    assert _table_counts(roster["roster_id"]) == (1, 3)

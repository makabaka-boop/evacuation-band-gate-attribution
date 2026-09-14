"""一次性验收：真实 PostgreSQL 并发事务。

运行方式（容器内由 verify 服务执行）：
    DATABASE_URL=postgresql+psycopg://evac:evac@db:5432/evac \
    API_BASE_URL=http://api:8000 \
    python -m pytest tests/acceptance -v

每个测试使用独立 band/event 前缀，互不干扰；并发均在 spawn 出来的独立
进程、独立数据库连接中进行。
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

from app.database import SessionLocal
from app.service import get_band_fact

from .conftest import (
    DATABASE_URL,
    barrier_submit_concurrently,
    count_concurrently,
    iso,
    result_sanity,
    run_in_process,
    submit_concurrently,
)

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


@pytest.fixture(scope="session", autouse=True)
def _tally_tables() -> None:
    """建立验收专用的清点表与屏障表（幂等）。"""
    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS zone_tally (
                    zone       TEXT NOT NULL,
                    band_id    TEXT NOT NULL,
                    event_id   TEXT NOT NULL,
                    PRIMARY KEY (zone, band_id)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS race_barrier (
                    barrier_id TEXT NOT NULL,
                    slot       TEXT NOT NULL,
                    arrived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (barrier_id, slot)
                )
                """
            )
        )
    engine.dispose()


@pytest.fixture
def ns() -> str:
    return uuid.uuid4().hex


def _payload(ns: str, *, event: str, gate: str, offset_minutes: int = 0) -> dict:
    ts = datetime(2026, 9, 14, 10, 0, 0, tzinfo=timezone.utc) + timedelta(
        minutes=offset_minutes
    )
    return {
        "event_id": f"evt-{ns}-{event}",
        "band_id": f"band-{ns}",
        "gate_id": gate,
        "scanned_at": iso(ts),
    }


# ---------------------------------------------------------------------------
# 1) 核心验收：多个相邻闸机的真实并发事务，清点方最终只增加一人
# ---------------------------------------------------------------------------


def test_concurrent_gates_count_exactly_one_person(ns: str) -> None:
    """四个独立进程在 DB 屏障集合后同时提交，分区清点最终恰为 1 人。"""
    payloads = [
        {
            **_payload(ns, event=f"g{i}", gate=f"GATE-{i}"),
            "_racers": 4,
            "_barrier": f"bar-tally-{ns}",
        }
        for i in range(4)
    ]
    results = count_concurrently(payloads, zone=f"ZONE-{ns}")

    assert len(results) == 4
    for result in results:
        result_sanity(result)

    winners = [r for r in results if r["result"] == "first_seen"]
    losers = [r for r in results if r["result"] == "already_seen"]
    assert len(winners) == 1, results
    assert len(losers) == 3, results

    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            tally = conn.execute(
                text("SELECT COUNT(*) FROM zone_tally WHERE zone = :z"),
                {"z": f"ZONE-{ns}"},
            ).scalar_one()
            fact_count = conn.execute(
                text("SELECT COUNT(*) FROM band_first_seen WHERE band_id = :b"),
                {"b": f"band-{ns}"},
            ).scalar_one()
            idem_count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM idempotent_requests "
                    "WHERE event_id = ANY(:ids)"
                ),
                {"ids": [p["event_id"] for p in payloads]},
            ).scalar_one()
    finally:
        engine.dispose()

    assert tally == 1, "清点方对同一腕带虚增了人数！"
    assert fact_count == 1, "band_first_seen 出现了多条首次事实！"
    assert idem_count == 4, "每个事件都应有自己的幂等记录"


# ---------------------------------------------------------------------------
# 2) 恰有一个 first_seen，其余 already_seen 且归属同一
# ---------------------------------------------------------------------------


def test_exactly_one_first_seen_with_shared_attribution(ns: str) -> None:
    payloads = [
        {**_payload(ns, event=f"e{i}", gate=f"GATE-{i}"), "_racers": 4}
        for i in range(4)
    ]
    results = barrier_submit_concurrently(f"bar-{ns}", payloads)

    assert len(results) == 4
    for result in results:
        result_sanity(result)

    bodies = [r["body"] for r in results]
    firsts = [b for b in bodies if b["result"] == "first_seen"]
    seen = [b for b in bodies if b["result"] == "already_seen"]
    assert len(firsts) == 1
    assert len(seen) == 3

    winner = firsts[0]
    winner_event = winner["event_id"]
    for body in seen:
        # 每个响应回显自己的 event_id……
        assert body["event_id"] != winner_event
        # ……但携带完全相同的归属事实。
        assert body["first_gate_id"] == winner["first_gate_id"]
        assert body["first_seen_at"] == winner["first_seen_at"]
        assert body["band_id"] == winner["band_id"]


# ---------------------------------------------------------------------------
# 3) 按 band_id 查询返回唯一事实
# ---------------------------------------------------------------------------


def test_band_query_returns_the_unique_fact(ns: str) -> None:
    # 串行制造一次首次扫描。
    result = run_in_process(
        "_worker_submit",
        _payload(ns, event="only", gate="GATE-A"),
    )
    result_sanity(result)
    expected = result["body"]

    session = SessionLocal()
    try:
        fact = get_band_fact(session, f"band-{ns}")
    finally:
        session.close()

    assert fact is not None
    assert fact.gate_id == expected["first_gate_id"]
    assert fact.event_id == expected["event_id"]
    assert fact.created_at == datetime.fromisoformat(
        expected["first_seen_at"].replace("Z", "+00:00")
    )


# ---------------------------------------------------------------------------
# 4) 持久化幂等：相同载荷重放返回原响应、不新增记录
# ---------------------------------------------------------------------------


def test_replay_same_payload_returns_original_without_new_row(ns: str) -> None:
    payload = _payload(ns, event="idem", gate="GATE-A")

    first = run_in_process(
        "_worker_submit",
        payload,
    )
    result_sanity(first)
    assert first["status"] == 200
    assert first["body"]["result"] == "first_seen"

    # 用新的独立进程（等价于服务重启后的新连接）重放。
    replay = run_in_process(
        "_worker_submit",
        dict(payload),
    )
    result_sanity(replay)
    assert replay["status"] == 200
    assert replay["body"] == first["body"], "重放必须逐字返回原响应"

    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            idem_rows = conn.execute(
                text("SELECT COUNT(*) FROM idempotent_requests WHERE event_id = :e"),
                {"e": payload["event_id"]},
            ).scalar_one()
            band_rows = conn.execute(
                text("SELECT COUNT(*) FROM band_first_seen WHERE band_id = :b"),
                {"b": payload["band_id"]},
            ).scalar_one()
    finally:
        engine.dispose()

    assert idem_rows == 1
    assert band_rows == 1


def test_replay_after_winning_remains_first_seen(ns: str) -> None:
    """胜出事件自身重放仍是 first_seen（原响应逐字不变）。"""
    payload = _payload(ns, event="winner-replay", gate="GATE-7")
    first = run_in_process(
        "_worker_submit",
        payload,
    )
    result_sanity(first)

    replay = run_in_process(
        "_worker_submit",
        dict(payload),
    )
    result_sanity(replay)
    assert replay["body"] == first["body"]
    assert replay["body"]["result"] == "first_seen"


# ---------------------------------------------------------------------------
# 5) 同 event_id 不同载荷 -> 409，归属不变
# ---------------------------------------------------------------------------


def test_same_event_different_payload_conflicts_and_preserves_fact(ns: str) -> None:
    original = _payload(ns, event="dup", gate="GATE-A")
    conflicting = _payload(ns, event="dup", gate="GATE-B")  # event_id 相同，闸门不同

    first = run_in_process(
        "_worker_submit",
        original,
    )
    result_sanity(first)
    assert first["body"]["result"] == "first_seen"

    conflict = run_in_process(
        "_worker_submit",
        conflicting,
    )
    assert conflict["ok"]
    assert conflict["status"] == 409

    # 即使换第三个事件让 GATE-B 正常上报，也只能 already_seen，归属不被改变。
    follow = _payload(ns, event="follow", gate="GATE-B")
    again = run_in_process(
        "_worker_submit",
        follow,
    )
    result_sanity(again)
    assert again["body"]["result"] == "already_seen"
    assert again["body"]["first_gate_id"] == "GATE-A"

    # 409 不得落任何记录。
    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            idem_rows = conn.execute(
                text("SELECT COUNT(*) FROM idempotent_requests WHERE event_id = :e"),
                {"e": original["event_id"]},
            ).scalar_one()
    finally:
        engine.dispose()
    assert idem_rows == 1


def test_same_event_different_timezone_spelling_is_409(ns: str) -> None:
    """同一事件换时区写法（同一时刻的 +00:00 与 Z）必须 409，归属不动。"""
    original = _payload(ns, event="tz", gate="GATE-A")  # ...+00:00

    first = run_in_process("_worker_submit", original)
    result_sanity(first)
    assert first["status"] == 200
    assert first["body"]["result"] == "first_seen"

    # 等价时刻改写成 Z 结尾，event_id 不变。
    respelled = dict(original)
    respelled["scanned_at"] = original["scanned_at"].replace("+00:00", "Z")
    assert respelled["scanned_at"] != original["scanned_at"]

    conflict = run_in_process("_worker_submit", respelled)
    assert conflict["ok"]
    assert conflict["status"] == 409
    # 409 响应回带原始载荷，可据此核对冲突原因。
    assert conflict["body"]["original_payload"]["scanned_at"] == original["scanned_at"]

    # 原载荷逐字重放仍然成功并返回原响应。
    replay = run_in_process("_worker_submit", dict(original))
    result_sanity(replay)
    assert replay["body"] == first["body"]

    # 归属与记录数不变。
    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            idem_rows = conn.execute(
                text("SELECT COUNT(*) FROM idempotent_requests WHERE event_id = :e"),
                {"e": original["event_id"]},
            ).scalar_one()
            gate = conn.execute(
                text("SELECT gate_id FROM band_first_seen WHERE band_id = :b"),
                {"b": f"band-{ns}"},
            ).scalar_one()
    finally:
        engine.dispose()
    assert idem_rows == 1
    assert gate == "GATE-A"


# ---------------------------------------------------------------------------
# 6) 不按客户端时间倒排：晚提交但声称更早的时刻，仍判 already_seen
# ---------------------------------------------------------------------------


def test_attribution_follows_commit_order_not_client_scanned_at(ns: str) -> None:
    # 先提交：声称 10:05
    late_clock = _payload(ns, event="late-clock", gate="GATE-LATE", offset_minutes=5)
    first = run_in_process(
        "_worker_submit",
        late_clock,
    )
    result_sanity(first)
    assert first["body"]["result"] == "first_seen"

    # 后提交：声称 10:00（更早），不能反客为主。
    early_clock = _payload(ns, event="early-clock", gate="GATE-EARLY", offset_minutes=0)
    second = run_in_process(
        "_worker_submit",
        early_clock,
    )
    result_sanity(second)
    assert second["body"]["result"] == "already_seen"
    assert second["body"]["first_gate_id"] == "GATE-LATE"
    # 归属事实中的 scanned_at 是获胜事务自己声明的时刻，不被后来者改写。
    winner_moment = datetime.fromisoformat(
        second["body"]["scanned_at"].replace("Z", "+00:00")
    )
    assert winner_moment == datetime.fromisoformat(late_clock["scanned_at"])


# ---------------------------------------------------------------------------
# 7) 同一 event_id 并发提交：至多一条成功，且响应一致（不出现双 409/双成功）
# ---------------------------------------------------------------------------


def test_concurrent_same_event_single_winner(ns: str) -> None:
    payload = _payload(ns, event="hot", gate="GATE-A")
    clones = [dict(payload) for _ in range(3)]
    results = submit_concurrently(clones)

    assert len(results) == 3
    statuses = sorted(r["status"] for r in results)
    # 咨询锁串行化：首个落库，其余全部逐字重放；不允许 409（载荷相同）。
    assert statuses == [200, 200, 200], results
    bodies = [r["body"] for r in results]
    assert all(b == bodies[0] for b in bodies)
    assert bodies[0]["result"] == "first_seen"


# ---------------------------------------------------------------------------
# 8) HTTP 端到端：直接打双 worker 的 API（如 verify 容器可访问 api 服务）
# ---------------------------------------------------------------------------


def test_http_race_against_live_api(ns: str) -> None:
    """跨进程经过真实 HTTP -> 双 uvicorn worker -> PostgreSQL 的端到端竞争。"""
    httpx = pytest.importorskip("httpx")
    from concurrent.futures import ThreadPoolExecutor

    payloads = [_payload(ns, event=f"http{i}", gate=f"GATE-{i}") for i in range(4)]

    def post(body: dict):
        with httpx.Client(base_url=API_BASE_URL, timeout=30) as client:
            resp = client.post("/scans", json=body)
            return resp.status_code, resp.json()

    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(post, payloads))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"API 不可达（{API_BASE_URL}）：{exc}")

    results_status = [code for code, _ in responses]
    assert results_status == [200] * 4
    outcomes = [body["result"] for _, body in responses]
    assert outcomes.count("first_seen") == 1
    assert outcomes.count("already_seen") == 3

    winner = next(body for _, body in responses if body["result"] == "first_seen")
    for _, body in responses:
        assert body["first_gate_id"] == winner["first_gate_id"]
        assert body["first_seen_at"] == winner["first_seen_at"]

    # 按 band_id 查询同一事实。
    with httpx.Client(base_url=API_BASE_URL, timeout=30) as client:
        q = client.get(f"/bands/band-{ns}")
    assert q.status_code == 200
    assert q.json()["gate_id"] == winner["first_gate_id"]
    assert q.json()["event_id"] == winner["event_id"]

    # 持久化重放：胜出事件逐字一致。
    winner_payload = next(p for p in payloads if p["event_id"] == winner["event_id"])
    with httpx.Client(base_url=API_BASE_URL, timeout=30) as client:
        replay = client.post("/scans", json=winner_payload)
    assert replay.status_code == 200
    assert replay.json() == winner

    # 同键不同载荷 409。
    conflict_payload = dict(winner_payload)
    conflict_payload["gate_id"] = "GATE-EVIL"
    with httpx.Client(base_url=API_BASE_URL, timeout=30) as client:
        conflict = client.post("/scans", json=conflict_payload)
    assert conflict.status_code == 409

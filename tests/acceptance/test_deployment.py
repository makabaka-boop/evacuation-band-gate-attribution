"""闸机增援派驻的一次性验收：真实 PostgreSQL、独立进程、独立连接。

覆盖：
* 派驻创建后处于待到岗，到岗确认推进为已到岗，响应携带完整派驻事实与阶段；
* 并发确认同一 deployment_id 恰有一个 200，其余 409，先到岗时间不被覆盖；
* 并发创建同一 deployment_id 恰有一个 201，其余 409，原派驻保留；
* 非法提交（空白标识/人员号/闸机号、无时区时间）422 且不留残行；
* 未知派驻确认/查询 404；派驻不读取也不改变巡检结论与扫描归属；
* HTTP 端到端全流程。

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
    confirm_arrival_concurrently,
    create_deployment_concurrently,
    iso,
    result_sanity,
    run_in_process,
)

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


@pytest.fixture
def ns() -> str:
    return uuid.uuid4().hex


def _ts(value: str) -> datetime:
    """把响应/载荷里的 ISO8601 字符串统一解析为可比较的 datetime。

    响应体经 Pydantic 序列化（UTC 写作 ``Z``），请求载荷与 409 回带的
    ``isoformat()`` 写作 ``+00:00`` —— 同一时刻两种写法，比较前统一解析。
    """
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _deploy_payload(
    ns: str,
    *,
    dep: str,
    responder: str | None = None,
    gate: str,
    offset_minutes: int = 0,
) -> dict:
    ts = datetime(2026, 9, 15, 9, 0, 0, tzinfo=timezone.utc) + timedelta(
        minutes=offset_minutes
    )
    return {
        "deployment_id": f"dep-{ns}-{dep}",
        "responder_id": responder if responder is not None else f"resp-{ns}",
        "gate_id": gate,
        "deployed_at": iso(ts),
    }


def _arrival_payload(offset_minutes: int = 0, **extra) -> dict:
    ts = datetime(2026, 9, 15, 9, 7, 0, tzinfo=timezone.utc) + timedelta(
        minutes=offset_minutes
    )
    return {"arrived_at": iso(ts), **extra}


def _deployment_rows(where: str, params: dict) -> list:
    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            return list(
                conn.execute(
                    text(
                        "SELECT deployment_id, responder_id, gate_id, "
                        "deployed_at, arrived_at "
                        f"FROM gate_deployments WHERE {where}"
                    ),
                    params,
                ).all()
            )
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# 1) 创建后可到岗：待到岗 -> 确认 -> 已到岗，响应携带完整事实与当前阶段
# ---------------------------------------------------------------------------


def test_create_then_confirm_arrival_full_fact(ns: str) -> None:
    gate = f"GATE-{ns}"
    payload = _deploy_payload(ns, dep="d1", gate=gate)

    created = run_in_process("_worker_create_deployment", payload)
    result_sanity(created)
    assert created["status"] == 201
    body = created["body"]
    # 待到岗记录：完整派驻事实 + 当前阶段。
    assert body["deployment_id"] == payload["deployment_id"]
    assert body["responder_id"] == payload["responder_id"]
    assert body["gate_id"] == gate
    assert _ts(body["deployed_at"]) == _ts(payload["deployed_at"])
    assert body["arrived_at"] is None
    assert body["phase"] == "pending"
    assert "recorded_at" in body

    arrival = _arrival_payload(offset_minutes=3)
    confirmed = run_in_process(
        "_worker_confirm_arrival", payload["deployment_id"], arrival
    )
    result_sanity(confirmed)
    assert confirmed["status"] == 200
    arrived = confirmed["body"]
    # 到岗确认返回同一完整事实，阶段推进为已到岗。
    assert arrived["deployment_id"] == payload["deployment_id"]
    assert arrived["responder_id"] == payload["responder_id"]
    assert arrived["gate_id"] == gate
    assert arrived["deployed_at"] == body["deployed_at"]
    assert arrived["recorded_at"] == body["recorded_at"]
    assert _ts(arrived["arrived_at"]) == _ts(arrival["arrived_at"])
    assert arrived["phase"] == "arrived"

    # 调度员可按 deployment_id 查询到同一已到岗事实。
    fetched = run_in_process("_worker_get_deployment", payload["deployment_id"])
    result_sanity(fetched)
    assert fetched["status"] == 200
    assert fetched["body"] == arrived

    # 数据库中恰有一行，到岗时间与确认请求声明的一致。
    rows = _deployment_rows("deployment_id = :d", {"d": payload["deployment_id"]})
    assert len(rows) == 1
    assert rows[0].arrived_at == _ts(arrival["arrived_at"])


# ---------------------------------------------------------------------------
# 2) 并发确认同一派驻：恰有一个 200，其余 409，先到岗时间不被覆盖
# ---------------------------------------------------------------------------


def test_concurrent_arrival_confirmations_single_winner(ns: str) -> None:
    gate = f"GATE-{ns}"
    payload = _deploy_payload(ns, dep="hot", gate=gate)
    created = run_in_process("_worker_create_deployment", payload)
    result_sanity(created)
    assert created["status"] == 201

    # 四个独立进程在数据库屏障集合后同时确认，各自声明不同的到岗时间。
    arrivals = [
        {**_arrival_payload(offset_minutes=i), "_racers": 4, "_slot": f"slot-{i}"}
        for i in range(4)
    ]
    results = confirm_arrival_concurrently(
        f"bar-dep-{ns}", payload["deployment_id"], arrivals
    )

    assert len(results) == 4
    for result in results:
        result_sanity(result)
    statuses = sorted(r["status"] for r in results)
    assert statuses == [200, 409, 409, 409], results

    winner = next(r for r in results if r["status"] == 200)["body"]
    assert winner["phase"] == "arrived"
    # 胜者的到岗时间正是四个声明值之一。
    assert _ts(winner["arrived_at"]) in {_ts(a["arrived_at"]) for a in arrivals}

    # 每个 409 都回带先到岗时间，且与胜者一致 —— 无人覆盖原值。
    for result in results:
        if result["status"] == 409:
            assert _ts(result["body"]["arrived_at"]) == _ts(winner["arrived_at"])

    # 数据库中的到岗时间即胜者声明的值；派驻事实其余字段不变。
    rows = _deployment_rows("deployment_id = :d", {"d": payload["deployment_id"]})
    assert len(rows) == 1
    assert rows[0].arrived_at == _ts(winner["arrived_at"])
    assert rows[0].responder_id == payload["responder_id"]
    assert rows[0].gate_id == gate

    # 事后再确认（哪怕声明更早的到岗时间）也只能 409，原值不动。
    late = run_in_process(
        "_worker_confirm_arrival",
        payload["deployment_id"],
        _arrival_payload(offset_minutes=-30),
    )
    result_sanity(late)
    assert late["status"] == 409
    assert _ts(late["body"]["arrived_at"]) == _ts(winner["arrived_at"])
    fetched = run_in_process("_worker_get_deployment", payload["deployment_id"])
    result_sanity(fetched)
    assert _ts(fetched["body"]["arrived_at"]) == _ts(winner["arrived_at"])
    assert fetched["body"]["phase"] == "arrived"


# ---------------------------------------------------------------------------
# 3) 并发创建同一 deployment_id：恰有一个 201，其余 409，原派驻保留
# ---------------------------------------------------------------------------


def test_concurrent_create_same_deployment_id_single_winner(ns: str) -> None:
    gate = f"GATE-{ns}"
    base = _deploy_payload(ns, dep="dup", gate=gate)
    clones = [
        {
            **base,
            "responder_id": f"resp-{ns}-{i}",
            "_racers": 3,
            "_slot": f"slot-{i}",
        }
        for i in range(3)
    ]
    results = create_deployment_concurrently(f"bar-dep-create-{ns}", clones)

    assert len(results) == 3
    for result in results:
        result_sanity(result)
    statuses = sorted(r["status"] for r in results)
    assert statuses == [201, 409, 409], results

    # 只有胜者的内容落库；落败的 409 不改写任何数据。
    winner = next(r for r in results if r["status"] == 201)["body"]
    rows = _deployment_rows("deployment_id = :d", {"d": base["deployment_id"]})
    assert len(rows) == 1
    assert rows[0].responder_id == winner["responder_id"]
    assert rows[0].arrived_at is None

    fetched = run_in_process("_worker_get_deployment", base["deployment_id"])
    result_sanity(fetched)
    assert fetched["body"] == winner
    assert fetched["body"]["phase"] == "pending"


# ---------------------------------------------------------------------------
# 4) 非法提交 422 且不留残行；未知派驻确认/查询 404
# ---------------------------------------------------------------------------


def test_invalid_submissions_422_and_leave_no_rows(ns: str) -> None:
    httpx = pytest.importorskip("httpx")

    gate = f"GATE-{ns}-invalid"
    base = _deploy_payload(ns, dep="bad", gate=gate)
    cases = [
        {**base, "deployment_id": ""},
        {**base, "deployment_id": "   "},
        {**base, "deployment_id": " \t\n "},
        {**base, "responder_id": ""},
        {**base, "responder_id": "  "},
        {**base, "gate_id": ""},
        {**base, "gate_id": " \t\n "},
        {**base, "deployed_at": "2026-09-15T09:00:00"},  # 无时区偏移
        {**base, "bogus": 1},  # 未知字段
    ]
    # 先建一条合法派驻，用于验证无时区到岗时间也在入库前 422。
    ok_dep = _deploy_payload(ns, dep="ok", gate=gate)
    arrival_cases = [
        {"arrived_at": "2026-09-15T09:07:00"},  # 无时区偏移
        {"arrived_at": ""},
        {},
        {**_arrival_payload(), "bogus": 1},  # 未知字段
    ]

    try:
        with httpx.Client(base_url=API_BASE_URL, timeout=30) as client:
            codes = [
                client.post("/deployments", json=body).status_code for body in cases
            ]
            assert client.post("/deployments", json=ok_dep).status_code == 201
            arrival_codes = [
                client.post(
                    f"/deployments/{ok_dep['deployment_id']}/arrival", json=body
                ).status_code
                for body in arrival_cases
            ]
            unknown_confirm = client.post(
                f"/deployments/dep-{ns}-unknown/arrival", json=_arrival_payload()
            )
            unknown_get = client.get(f"/deployments/dep-{ns}-unknown")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"API 不可达（{API_BASE_URL}）：{exc}")

    assert codes == [422] * len(cases)
    assert arrival_codes == [422] * len(arrival_cases)
    assert unknown_confirm.status_code == 404
    assert unknown_get.status_code == 404

    # 非法提交不得在派驻表留下任何残行：该闸机与本前缀下都只有合法派驻一行，
    # 且合法派驻未被任何非法确认推进（仍待到岗）。
    gate_rows = _deployment_rows("gate_id = :g", {"g": gate})
    assert [row.deployment_id for row in gate_rows] == [ok_dep["deployment_id"]]
    rows = _deployment_rows("deployment_id LIKE :p", {"p": f"dep-{ns}-%"})
    assert [row.deployment_id for row in rows] == [ok_dep["deployment_id"]]
    assert rows[0].arrived_at is None

    # 服务层确认未知派驻同样为“不存在”，且不留残行。
    result = run_in_process(
        "_worker_confirm_arrival", f"dep-{ns}-unknown", _arrival_payload()
    )
    result_sanity(result)
    assert result["status"] == 404
    assert _deployment_rows("deployment_id LIKE :p", {"p": f"dep-{ns}-unknown%"}) == []
    fetched = run_in_process("_worker_get_deployment", f"dep-{ns}-unknown")
    result_sanity(fetched)
    assert fetched["status"] == 404


# ---------------------------------------------------------------------------
# 5) 派驻不读取也不改变巡检结论与扫描归属
# ---------------------------------------------------------------------------


def test_deployment_does_not_touch_inspection_or_scan(ns: str) -> None:
    gate = f"GATE-{ns}"

    # 闸机被标记故障后，派驻与到岗确认照常进行（派驻不读巡检结论）。
    faulty = run_in_process(
        "_worker_submit_inspection",
        {
            "inspection_id": f"insp-{ns}-faulty",
            "gate_id": gate,
            "checked_at": iso(datetime(2026, 9, 15, 8, 30, tzinfo=timezone.utc)),
            "conclusion": "faulty",
        },
    )
    result_sanity(faulty)
    assert faulty["status"] == 201

    payload = _deploy_payload(ns, dep="on-faulty", gate=gate)
    created = run_in_process("_worker_create_deployment", payload)
    result_sanity(created)
    assert created["status"] == 201
    confirmed = run_in_process(
        "_worker_confirm_arrival", payload["deployment_id"], _arrival_payload()
    )
    result_sanity(confirmed)
    assert confirmed["status"] == 200
    assert confirmed["body"]["phase"] == "arrived"

    # 派驻与确认不改变巡检结论：故障状态如实保持。
    latest = run_in_process("_worker_latest_inspection", gate)
    result_sanity(latest)
    assert latest["body"]["status"] == "faulty"
    assert latest["body"]["latest_inspection"]["inspection_id"] == f"insp-{ns}-faulty"

    # 既有扫描归属也不受派驻影响。
    scan = run_in_process(
        "_worker_submit",
        {
            "event_id": f"evt-{ns}-scan",
            "band_id": f"band-{ns}",
            "gate_id": gate,
            "scanned_at": iso(datetime(2026, 9, 15, 10, 0, tzinfo=timezone.utc)),
        },
    )
    result_sanity(scan)
    assert scan["body"]["result"] == "first_seen"
    assert scan["body"]["first_gate_id"] == gate


# ---------------------------------------------------------------------------
# 6) HTTP 端到端：派驻 -> 查询 -> 到岗确认 -> 重复 409 -> 未知 404
# ---------------------------------------------------------------------------


def test_http_deployment_flow_against_live_api(ns: str) -> None:
    """跨进程经真实 HTTP -> 双 uvicorn worker -> PostgreSQL 的派驻全流程。"""
    httpx = pytest.importorskip("httpx")

    gate = f"GATE-{ns}"
    payload = _deploy_payload(ns, dep="http", gate=gate)
    arrival = _arrival_payload(offset_minutes=5)

    try:
        with httpx.Client(base_url=API_BASE_URL, timeout=30) as client:
            created = client.post("/deployments", json=payload)
            fetched_pending = client.get(f"/deployments/{payload['deployment_id']}")
            confirmed = client.post(
                f"/deployments/{payload['deployment_id']}/arrival", json=arrival
            )
            fetched_arrived = client.get(f"/deployments/{payload['deployment_id']}")
            duplicate = client.post(
                "/deployments", json={**payload, "responder_id": f"resp-{ns}-impostor"}
            )
            again = client.post(
                f"/deployments/{payload['deployment_id']}/arrival",
                json=_arrival_payload(offset_minutes=99),
            )
            after_all = client.get(f"/deployments/{payload['deployment_id']}")
            unknown_confirm = client.post(
                f"/deployments/dep-{ns}-unknown/arrival", json=arrival
            )
            unknown_get = client.get(f"/deployments/dep-{ns}-unknown")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"API 不可达（{API_BASE_URL}）：{exc}")

    assert created.status_code == 201
    body = created.json()
    assert body["phase"] == "pending"
    assert body["arrived_at"] is None
    assert fetched_pending.status_code == 200
    assert fetched_pending.json() == body

    assert confirmed.status_code == 200
    arrived = confirmed.json()
    assert arrived["phase"] == "arrived"
    assert _ts(arrived["arrived_at"]) == _ts(arrival["arrived_at"])
    # 完整事实：除到岗时间与阶段外，其余字段与派驻时逐字一致。
    assert arrived["deployment_id"] == body["deployment_id"]
    assert arrived["responder_id"] == body["responder_id"]
    assert arrived["gate_id"] == body["gate_id"]
    assert arrived["deployed_at"] == body["deployed_at"]
    assert arrived["recorded_at"] == body["recorded_at"]
    assert fetched_arrived.status_code == 200
    assert fetched_arrived.json() == arrived

    # 重复 deployment_id -> 409，原派驻分毫不动。
    assert duplicate.status_code == 409
    assert duplicate.json()["deployment_id"] == payload["deployment_id"]

    # 重复确认 -> 409，回带先到岗时间且不覆盖原值。
    assert again.status_code == 409
    assert _ts(again.json()["arrived_at"]) == _ts(arrival["arrived_at"])
    assert after_all.status_code == 200
    assert after_all.json() == arrived

    assert unknown_confirm.status_code == 404
    assert unknown_get.status_code == 404

    # 全流程只落一行派驻记录。
    rows = _deployment_rows("deployment_id = :d", {"d": payload["deployment_id"]})
    assert len(rows) == 1
    assert rows[0].arrived_at == _ts(arrival["arrived_at"])

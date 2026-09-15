"""请求关联标识（X-Request-ID）的一次性验收：真实 API（双 worker）+ 真实 PostgreSQL。

覆盖：
* 并发调用扫描与健康检查：自带标识逐字原样传播（响应头回显）；
* 缺省标识彼此唯一且均为合法格式，入口日志可按 request_id 关联（进程内证明）；
* 非法标识在进入路由与创建数据库会话前 400：响应头与错误正文携带新生成的
  可追踪标识，且不触发任何业务写入（库中无残行、腕带查询 404）；
* 既有 404/409/422 也带响应头，状态码与 JSON 字段不变；
* 扫描归属、名册核对、巡检、派驻的既有响应契约不受影响
  （API_PORT 启动方式不变：验收仍经 API_BASE_URL 直连服务）。

运行方式与 test_evacuation.py 相同（由 verify 服务执行）。
"""
from __future__ import annotations

import logging
import os
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text

from .conftest import DATABASE_URL, iso

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

#: 响应头读取键（httpx 对响应头大小写不敏感，统一小写）。
HDR = "x-request-id"
VALID = re.compile(r"[A-Za-z0-9._-]{1,64}")


@pytest.fixture
def ns() -> str:
    return uuid.uuid4().hex


@pytest.fixture(scope="module")
def http():
    httpx = pytest.importorskip("httpx")
    client = httpx.Client(base_url=API_BASE_URL, timeout=30)
    try:
        client.get("/health")
    except Exception as exc:  # noqa: BLE001
        client.close()
        pytest.skip(f"API 不可达（{API_BASE_URL}）：{exc}")
    yield client
    client.close()


def _scan_payload(
    ns: str, *, event: str, band: str | None = None, gate: str = "GATE-R"
) -> dict:
    return {
        "event_id": f"evt-{ns}-{event}",
        "band_id": band if band is not None else f"band-{ns}-{event}",
        "gate_id": gate,
        "scanned_at": iso(datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)),
    }


# ---------------------------------------------------------------------------
# 1) 并发调用扫描与健康检查：自带标识原样传播
# ---------------------------------------------------------------------------


def test_provided_ids_propagate_verbatim_under_concurrency(http, ns: str) -> None:
    rid_scans = [f"scan.{i:02d}_{ns[:8]}-X" for i in range(4)]
    rid_healths = ["h", "Z" * 64, f"health_{ns[:8]}-1.2"]

    def do_scan(i: int):
        return http.post(
            "/scans",
            json=_scan_payload(ns, event=f"p{i}"),
            headers={"X-Request-ID": rid_scans[i]},
        )

    def do_health(i: int):
        return http.get("/health", headers={"X-Request-ID": rid_healths[i]})

    with ThreadPoolExecutor(max_workers=7) as pool:
        futures = [pool.submit(do_scan, i) for i in range(4)]
        futures += [pool.submit(do_health, i) for i in range(3)]
        responses = [f.result() for f in futures]

    scans, healths = responses[:4], responses[4:]
    for i, resp in enumerate(scans):
        assert resp.status_code == 200
        assert resp.headers[HDR] == rid_scans[i]  # 原样传播
        body = resp.json()
        # 既有扫描契约不变：各自独立腕带均为 first_seen，字段集合不变。
        assert body["result"] == "first_seen"
        assert set(body) == {
            "event_id",
            "band_id",
            "gate_id",
            "scanned_at",
            "first_gate_id",
            "first_seen_at",
            "result",
        }
    for i, resp in enumerate(healths):
        assert resp.status_code == 200
        assert resp.headers[HDR] == rid_healths[i]
        assert resp.json() == {"status": "ok"}  # 健康检查正文保持原样


# ---------------------------------------------------------------------------
# 2) 缺省标识：并发下彼此唯一且均为合法格式
# ---------------------------------------------------------------------------


def test_generated_ids_unique_across_concurrent_requests(http, ns: str) -> None:
    def do_scan(i: int):
        return http.post("/scans", json=_scan_payload(ns, event=f"g{i}"))

    def do_health(_: int):
        return http.get("/health")

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(do_scan, i) for i in range(4)]
        futures += [pool.submit(do_health, i) for i in range(4)]
        responses = [f.result() for f in futures]

    assert all(r.status_code == 200 for r in responses)
    ids = [r.headers[HDR] for r in responses]
    assert all(VALID.fullmatch(i) for i in ids)
    assert len(set(ids)) == len(ids), "缺省标识在并发下必须彼此唯一、互不串号"


# ---------------------------------------------------------------------------
# 3) 非法标识：进入路由与建库会话前 400，不触发任何业务写入
# ---------------------------------------------------------------------------


def test_invalid_ids_400_and_no_business_write(http, ns: str) -> None:
    # 非 ASCII 值以原始字节送达（httpx 的 str 头按 ASCII 编码，curl 可发任意字节）。
    invalids = ["", "has space", "slash/in", "x" * 65, "semi;colon"]
    raw_invalids = [[(b"x-request-id", "中文id".encode("utf-8"))]]
    fresh_ids: list[str] = []
    engine = create_engine(DATABASE_URL)
    try:
        for i, bad in enumerate(invalids + raw_invalids):
            payload = _scan_payload(ns, event=f"bad{i}")
            headers = (
                {"X-Request-ID": bad} if isinstance(bad, str) else bad
            )
            resp = http.post("/scans", json=payload, headers=headers)
            assert resp.status_code == 400
            body = resp.json()
            # 响应头与错误正文携带同一个新生成的可追踪标识。
            assert VALID.fullmatch(body["request_id"])
            assert resp.headers[HDR] == body["request_id"]
            fresh_ids.append(body["request_id"])

            # 不触发任何业务写入：幂等记录与归属事实都无残行，腕带查询 404。
            with engine.connect() as conn:
                idem = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM idempotent_requests "
                        "WHERE event_id = :e"
                    ),
                    {"e": payload["event_id"]},
                ).scalar_one()
                band = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM band_first_seen WHERE band_id = :b"
                    ),
                    {"b": payload["band_id"]},
                ).scalar_one()
            assert (idem, band) == (0, 0)
            assert http.get(f"/bands/{payload['band_id']}").status_code == 404
    finally:
        engine.dispose()

    # 每次拒绝都生成新的可追踪标识。
    assert len(set(fresh_ids)) == len(fresh_ids)

    # 健康检查同样在进入路由前 400。
    assert http.get("/health", headers={"X-Request-ID": "bad id"}).status_code == 400


# ---------------------------------------------------------------------------
# 4) 既有错误路径：响应头补齐，状态码与 JSON 字段不变
# ---------------------------------------------------------------------------


def test_error_responses_carry_header_and_keep_contract(http, ns: str) -> None:
    rid = f"err-{ns[:16]}"
    headers = {"X-Request-ID": rid}

    # 404：未知腕带。
    resp = http.get(f"/bands/nope-{ns}", headers=headers)
    assert resp.status_code == 404
    assert resp.headers[HDR] == rid
    assert resp.json() == {"detail": "band has no recorded scan"}

    # 409：同 event_id 不同载荷。
    payload = _scan_payload(ns, event="conflict")
    assert http.post("/scans", json=payload).status_code == 200
    conflict = http.post(
        "/scans", json={**payload, "gate_id": "GATE-OTHER"}, headers=headers
    )
    assert conflict.status_code == 409
    assert conflict.headers[HDR] == rid
    assert set(conflict.json()) == {"detail", "original_payload"}

    # 422：非法载荷。
    bad = http.post("/scans", json={"event_id": "evt-x"}, headers=headers)
    assert bad.status_code == 422
    assert bad.headers[HDR] == rid
    assert isinstance(bad.json()["detail"], list)

    # 未匹配路由 404。
    missing = http.get("/no-such-route", headers=headers)
    assert missing.status_code == 404
    assert missing.headers[HDR] == rid
    assert missing.json() == {"detail": "Not Found"}


# ---------------------------------------------------------------------------
# 5) 既有响应契约不受影响：扫描归属 / 名册核对 / 巡检 / 派驻
# ---------------------------------------------------------------------------


def test_existing_domain_contracts_unaffected(http, ns: str) -> None:
    rid = f"dom-{ns[:16]}"
    headers = {"X-Request-ID": rid}

    # 扫描归属：同一腕带两台闸机，恰一个 first_seen，归属一致。
    band = f"band-{ns}-shared"
    first = http.post(
        "/scans", json=_scan_payload(ns, event="d0", band=band, gate="GATE-A"),
        headers=headers,
    )
    second = http.post(
        "/scans", json=_scan_payload(ns, event="d1", band=band, gate="GATE-B"),
        headers=headers,
    )
    assert [r.status_code for r in (first, second)] == [200, 200]
    assert [r.headers[HDR] for r in (first, second)] == [rid, rid]
    assert first.json()["result"] == "first_seen"
    assert second.json()["result"] == "already_seen"
    assert second.json()["first_gate_id"] == "GATE-A"
    assert second.json()["first_seen_at"] == first.json()["first_seen_at"]

    # 名册核对：创建 201 + 核对差分精确。
    roster = {
        "roster_id": f"roster-{ns}",
        "name": "演练名册",
        "band_ids": [band, f"band-{ns}-other"],
    }
    created = http.post("/rosters", json=roster, headers=headers)
    assert created.status_code == 201
    assert created.headers[HDR] == rid
    assert created.json() == {
        "roster_id": roster["roster_id"],
        "name": "演练名册",
        "expected_count": 2,
    }
    check = http.get(f"/rosters/{roster['roster_id']}", headers=headers)
    assert check.status_code == 200
    assert check.headers[HDR] == rid
    assert check.json()["passed_count"] == 1
    assert check.json()["missing_band_ids"] == [f"band-{ns}-other"]

    # 巡检：提交 201 + 最近一次查询。
    gate = f"GATE-{ns}"
    insp = {
        "inspection_id": f"insp-{ns}",
        "gate_id": gate,
        "checked_at": iso(datetime(2026, 9, 14, 8, 0, tzinfo=timezone.utc)),
        "conclusion": "available",
    }
    created_i = http.post("/inspections", json=insp, headers=headers)
    assert created_i.status_code == 201
    assert created_i.headers[HDR] == rid
    latest = http.get(f"/gates/{gate}/inspections/latest", headers=headers)
    assert latest.status_code == 200
    assert latest.headers[HDR] == rid
    assert latest.json()["status"] == "available"
    assert latest.json()["latest_inspection"]["inspection_id"] == f"insp-{ns}"

    # 派驻：创建 201（待到岗）+ 到岗确认 200（已到岗）。
    dep = {
        "deployment_id": f"dep-{ns}",
        "responder_id": f"resp-{ns}",
        "gate_id": gate,
        "deployed_at": iso(datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc)),
    }
    created_d = http.post("/deployments", json=dep, headers=headers)
    assert created_d.status_code == 201
    assert created_d.headers[HDR] == rid
    assert created_d.json()["phase"] == "pending"
    assert created_d.json()["arrived_at"] is None
    arrival = http.post(
        f"/deployments/{dep['deployment_id']}/arrival",
        json={"arrived_at": iso(datetime(2026, 9, 15, 9, 7, tzinfo=timezone.utc))},
        headers=headers,
    )
    assert arrival.status_code == 200
    assert arrival.headers[HDR] == rid
    assert arrival.json()["phase"] == "arrived"
    assert arrival.json()["arrived_at"] is not None


# ---------------------------------------------------------------------------
# 6) 日志可关联：入口日志携带与响应头一致的 request_id（进程内证明）
# ---------------------------------------------------------------------------


def test_logs_correlatable_by_request_id(caplog) -> None:
    from fastapi.testclient import TestClient

    from app.main import app
    from app.request_context import RequestIdFilter

    caplog.handler.addFilter(RequestIdFilter())
    # 不作为上下文管理器进入：不运行 lifespan，健康检查无需数据库。
    client = TestClient(app)
    rid = f"log-{uuid.uuid4().hex[:16]}"
    with caplog.at_level(logging.INFO, logger="app"):
        resp = client.get("/health", headers={"X-Request-ID": rid})
    assert resp.status_code == 200
    assert resp.headers[HDR] == rid
    assert any(
        getattr(r, "request_id", None) == rid for r in caplog.records
    ), "入口日志应携带与响应头一致的 request_id"

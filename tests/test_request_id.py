"""请求关联标识（X-Request-ID）的 API 级测试。

中间件行为（格式校验 400、响应头回写、日志关联、并发隔离与上下文清理）
不依赖数据库；涉及业务写入的用例在数据库不可达时整体跳过
（与 tests/test_api.py 一致）。
"""
from __future__ import annotations

import asyncio
import logging
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import engine, get_session
from app.request_context import (
    REQUEST_ID_HEADER,
    RequestIdFilter,
    RequestIdMiddleware,
    current_request_id,
    generate_request_id,
    is_valid_request_id,
)

#: 响应头读取键（httpx 对响应头大小写不敏感，统一小写）。
HDR = "x-request-id"
VALID = re.compile(r"[A-Za-z0-9._-]{1,64}")


@pytest.fixture
def client():
    """不触发 lifespan（建表）的客户端：覆盖不依赖数据库的中间件行为。"""
    from fastapi.testclient import TestClient

    from app.main import app

    # 不作为上下文管理器进入：不运行 lifespan（不触碰数据库）。
    c = TestClient(app)
    yield c
    c.close()


@pytest.fixture
def db_client():
    """直连真实 PostgreSQL 的客户端；不可达则跳过（同 test_api.py）。"""
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


# ---------------------------------------------------------------------------
# 标识格式与生成
# ---------------------------------------------------------------------------


class TestFormat:
    def test_pattern_boundaries(self) -> None:
        assert is_valid_request_id("a")  # 下限 1 位
        assert is_valid_request_id("Z" * 64)  # 上限 64 位
        assert is_valid_request_id("req_01.2-ABC")
        assert not is_valid_request_id("")  # 至少 1 位
        assert not is_valid_request_id("x" * 65)  # 超长
        assert not is_valid_request_id("has space")
        assert not is_valid_request_id("slash/in")
        assert not is_valid_request_id("semi;colon")
        assert not is_valid_request_id("中文标识")
        assert not is_valid_request_id("trail\n")

    def test_generated_ids_valid_and_unique(self) -> None:
        ids = {generate_request_id() for _ in range(1000)}
        assert len(ids) == 1000
        assert all(VALID.fullmatch(i) for i in ids)


# ---------------------------------------------------------------------------
# 响应头：自带标识原样传播、缺省标识生成
# ---------------------------------------------------------------------------


class TestResponseHeader:
    @pytest.mark.parametrize("rid", ["a", "Z" * 64, "req_01.2-ABC", "0"])
    def test_provided_id_echoed_verbatim(self, client, rid: str) -> None:
        resp = client.get("/health", headers={REQUEST_ID_HEADER: rid})
        assert resp.status_code == 200
        assert resp.headers[HDR] == rid  # 原样传播
        assert resp.json() == {"status": "ok"}  # 成功正文保持原样

    def test_generated_when_absent_and_unique(self, client) -> None:
        first = client.get("/health")
        second = client.get("/health")
        assert first.status_code == second.status_code == 200
        assert VALID.fullmatch(first.headers[HDR])
        assert VALID.fullmatch(second.headers[HDR])
        # 缺省标识彼此唯一，不串号。
        assert first.headers[HDR] != second.headers[HDR]
        assert first.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# 非法标识：进入路由与创建数据库会话之前 400
# ---------------------------------------------------------------------------


class TestInvalidRequestId:
    INVALID = [
        "",
        "has space",
        "slash/in",
        "x" * 65,
        "semi;colon",
        "perc%ent",
        "tab\tchar",
    ]

    @pytest.mark.parametrize("bad", INVALID)
    def test_400_with_fresh_traceable_id(self, client, bad: str) -> None:
        resp = client.get("/health", headers={REQUEST_ID_HEADER: bad})
        assert resp.status_code == 400
        body = resp.json()
        assert "detail" in body
        # 响应头与错误正文携带同一个新生成的可追踪标识。
        assert VALID.fullmatch(body["request_id"])
        assert resp.headers[HDR] == body["request_id"]

    def test_non_ascii_header_bytes_400(self, client) -> None:
        # httpx 的 str 头按 ASCII 编码，非 ASCII 需以原始字节送达（curl 等同理）。
        resp = client.get(
            "/health", headers=[(b"x-request-id", "中文id".encode("utf-8"))]
        )
        assert resp.status_code == 400
        body = resp.json()
        assert VALID.fullmatch(body["request_id"])
        assert resp.headers[HDR] == body["request_id"]

    def test_rejected_before_routing(self, client) -> None:
        # 不存在的路径同样 400（而不是 404）：校验发生在进入路由之前。
        resp = client.get("/no-such-route", headers={REQUEST_ID_HEADER: "bad id"})
        assert resp.status_code == 400

    def test_each_rejection_gets_distinct_fresh_id(self, client) -> None:
        ids = {
            client.get(
                "/health", headers={REQUEST_ID_HEADER: "bad id"}
            ).json()["request_id"]
            for _ in range(5)
        }
        assert len(ids) == 5


# ---------------------------------------------------------------------------
# 既有错误路径：响应头补齐，状态码与 JSON 字段不变
# ---------------------------------------------------------------------------


class TestErrorPathsCarryHeader:
    def test_unknown_route_404(self, client) -> None:
        resp = client.get("/no-such-route", headers={REQUEST_ID_HEADER: "rid-404"})
        assert resp.status_code == 404
        assert resp.headers[HDR] == "rid-404"
        assert resp.json() == {"detail": "Not Found"}  # 原 JSON 字段不变

    def test_validation_422(self, client) -> None:
        resp = client.post(
            "/scans",
            json={"event_id": "evt-incomplete"},
            headers={REQUEST_ID_HEADER: "rid-422"},
        )
        assert resp.status_code == 422
        assert resp.headers[HDR] == "rid-422"
        assert isinstance(resp.json()["detail"], list)  # 原 422 结构不变


# ---------------------------------------------------------------------------
# 未捕获异常：500 与框架默认一致，仅补响应头（迷你应用，无需数据库）
# ---------------------------------------------------------------------------


def _mini_app():
    from fastapi import FastAPI

    mini = FastAPI()
    mini.add_middleware(RequestIdMiddleware)

    @mini.get("/boom")
    def boom():
        raise RuntimeError("kaboom")

    @mini.get("/echo")
    async def echo():
        await asyncio.sleep(0.05)  # 制造并发交错窗口
        return {"request_id": current_request_id()}

    return mini


class TestUncaughtException:
    def test_500_keeps_default_body_and_carries_header(self) -> None:
        from fastapi.testclient import TestClient

        client = TestClient(_mini_app())
        resp = client.get("/boom", headers={REQUEST_ID_HEADER: "rid-500"})
        assert resp.status_code == 500
        assert resp.text == "Internal Server Error"  # 与框架默认一致
        assert resp.headers[HDR] == "rid-500"

    def test_500_without_provided_id_gets_generated_header(self) -> None:
        from fastapi.testclient import TestClient

        client = TestClient(_mini_app())
        resp = client.get("/boom")
        assert resp.status_code == 500
        assert VALID.fullmatch(resp.headers[HDR])


# ---------------------------------------------------------------------------
# 上下文：日志关联、并发隔离、请求间清理
# ---------------------------------------------------------------------------


class TestContext:
    def test_logs_carry_request_id(self, client, caplog) -> None:
        caplog.handler.addFilter(RequestIdFilter())
        with caplog.at_level(logging.INFO, logger="app"):
            resp = client.get("/health", headers={REQUEST_ID_HEADER: "rid-log"})
        assert resp.headers[HDR] == "rid-log"
        matched = [
            r for r in caplog.records if getattr(r, "request_id", None) == "rid-log"
        ]
        assert matched, "入口日志应携带与响应头一致的 request_id"

    def test_generated_id_also_logged(self, client, caplog) -> None:
        caplog.handler.addFilter(RequestIdFilter())
        with caplog.at_level(logging.INFO, logger="app"):
            resp = client.get("/health")
        rid = resp.headers[HDR]
        assert any(getattr(r, "request_id", None) == rid for r in caplog.records)

    def test_business_layer_log_auto_tagged(self, caplog) -> None:
        """业务处理中的日志（请求上下文内任意日志点）自动携带 request_id。"""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        mini = FastAPI()
        mini.add_middleware(RequestIdMiddleware)
        biz_logger = logging.getLogger("app")

        @mini.get("/biz")
        def biz():
            biz_logger.info("business checkpoint")
            return {"ok": True}

        caplog.handler.addFilter(RequestIdFilter())
        client = TestClient(mini)
        with caplog.at_level(logging.INFO, logger="app"):
            resp = client.get("/biz", headers={REQUEST_ID_HEADER: "rid-biz"})
        assert resp.status_code == 200
        matched = [
            r
            for r in caplog.records
            if r.getMessage() == "business checkpoint"
        ]
        assert matched
        assert all(getattr(r, "request_id", None) == "rid-biz" for r in matched)

    def test_provided_id_does_not_leak_into_next_request(self, client) -> None:
        first = client.get("/health", headers={REQUEST_ID_HEADER: "rid-first"})
        assert first.headers[HDR] == "rid-first"
        second = client.get("/health")
        # 上一请求的自带标识不得残留到后续请求。
        assert second.headers[HDR] != "rid-first"
        assert VALID.fullmatch(second.headers[HDR])

    def test_concurrent_requests_do_not_cross_talk(self) -> None:
        from fastapi.testclient import TestClient

        client = TestClient(_mini_app())
        rids = [f"rid-{i:02d}" for i in range(8)]

        def call(rid: str) -> str:
            resp = client.get("/echo", headers={REQUEST_ID_HEADER: rid})
            assert resp.status_code == 200
            assert resp.headers[HDR] == rid
            return resp.json()["request_id"]

        with ThreadPoolExecutor(max_workers=8) as pool:
            got = list(pool.map(call, rids))
        # 并发请求各自取回自己的标识，互不串号。
        assert got == rids


# ---------------------------------------------------------------------------
# 数据库会话：请求结束前沿用同一上下文（会话对象惰性连接，无需真实 SQL）
# ---------------------------------------------------------------------------


class TestDbSessionContext:
    def _session_app(self):
        from fastapi import Depends, FastAPI
        from sqlalchemy.orm import Session

        mini = FastAPI()
        mini.add_middleware(RequestIdMiddleware)

        @mini.get("/session-context")
        def session_context(session: Session = Depends(get_session)):
            # 不执行任何 SQL：仅验证会话沿用了请求上下文。
            return {"session_request_id": session.info.get("request_id")}

        return mini

    def test_session_inherits_request_id(self) -> None:
        from fastapi.testclient import TestClient

        client = TestClient(self._session_app())
        resp = client.get("/session-context", headers={REQUEST_ID_HEADER: "rid-db"})
        assert resp.status_code == 200
        assert resp.json()["session_request_id"] == "rid-db"

    def test_session_logs_carry_request_id(self, caplog) -> None:
        from fastapi.testclient import TestClient

        caplog.handler.addFilter(RequestIdFilter())
        client = TestClient(self._session_app())
        with caplog.at_level(logging.DEBUG, logger="app.database"):
            resp = client.get("/session-context", headers={REQUEST_ID_HEADER: "rid-dblog"})
        assert resp.status_code == 200
        opened = [
            r for r in caplog.records if "db session opened" in r.getMessage()
        ]
        assert opened
        assert all(getattr(r, "request_id", None) == "rid-dblog" for r in opened)


# ---------------------------------------------------------------------------
# 业务契约（需要真实 PostgreSQL；不可达则跳过）
# ---------------------------------------------------------------------------


def _scan_payload(ns: str, *, event: str, band: str | None = None, gate: str = "G1") -> dict:
    return {
        "event_id": f"evt-{ns}-{event}",
        "band_id": band if band is not None else f"band-{ns}-{event}",
        "gate_id": gate,
        "scanned_at": datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc).isoformat(),
    }


class TestBusinessContract:
    def test_scan_body_unchanged_and_header_echoed(self, db_client, ns: str) -> None:
        payload = _scan_payload(ns, event="ok")
        resp = db_client.post(
            "/scans", json=payload, headers={REQUEST_ID_HEADER: f"rid-{ns}"}
        )
        assert resp.status_code == 200
        assert resp.headers[HDR] == f"rid-{ns}"
        # 成功响应正文保持原样：字段集合与既有契约一致。
        assert set(resp.json()) == {
            "event_id",
            "band_id",
            "gate_id",
            "scanned_at",
            "first_gate_id",
            "first_seen_at",
            "result",
        }
        assert resp.json()["result"] == "first_seen"

        # 幂等重放：正文逐字一致，未传标识时返回新生成的标识。
        replay = db_client.post("/scans", json=payload)
        assert replay.status_code == 200
        assert replay.json() == resp.json()
        assert VALID.fullmatch(replay.headers[HDR])

    def test_409_carries_header_and_original_fields(self, db_client, ns: str) -> None:
        payload = _scan_payload(ns, event="dup")
        assert db_client.post("/scans", json=payload).status_code == 200

        conflict = db_client.post(
            "/scans",
            json=_scan_payload(ns, event="dup", gate="G2"),
            headers={REQUEST_ID_HEADER: "rid-409"},
        )
        assert conflict.status_code == 409
        assert conflict.headers[HDR] == "rid-409"
        assert set(conflict.json()) == {"detail", "original_payload"}

    def test_404_carries_header_and_original_body(self, db_client, ns: str) -> None:
        resp = db_client.get(
            f"/bands/nope-{ns}", headers={REQUEST_ID_HEADER: "rid-404b"}
        )
        assert resp.status_code == 404
        assert resp.headers[HDR] == "rid-404b"
        assert resp.json() == {"detail": "band has no recorded scan"}

    def test_invalid_id_triggers_no_business_write(self, db_client, ns: str) -> None:
        payload = _scan_payload(ns, event="bad", band=f"band-{ns}-bad")
        resp = db_client.post(
            "/scans", json=payload, headers={REQUEST_ID_HEADER: "bad id!"}
        )
        assert resp.status_code == 400

        # 非法标识不触发任何业务写入：幂等记录与归属事实都无残行。
        with engine.connect() as conn:
            idem = conn.execute(
                text("SELECT COUNT(*) FROM idempotent_requests WHERE event_id = :e"),
                {"e": payload["event_id"]},
            ).scalar_one()
            band = conn.execute(
                text("SELECT COUNT(*) FROM band_first_seen WHERE band_id = :b"),
                {"b": payload["band_id"]},
            ).scalar_one()
        assert (idem, band) == (0, 0)
        assert db_client.get(f"/bands/{payload['band_id']}").status_code == 404

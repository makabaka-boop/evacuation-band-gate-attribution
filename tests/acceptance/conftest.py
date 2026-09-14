"""验收测试的共享工具。

所有并发验收都使用**独立进程 + 独立数据库连接**，从而真正覆盖跨进程、
跨请求的事务竞争（而非线程内共享会话的假并发）。

注意：multiprospawning 使用 spawn，所有 Process target 必须是本模块的
顶层可 pickle 函数。
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from multiprocessing import get_context
from typing import Any

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://evac:evac@localhost:5432/evac",
)


def iso(dt: datetime) -> str:
    """生成带偏移的 ISO8601 字符串。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


# ---------------------------------------------------------------------------
# 子进程入口（必须定义在模块顶层，spawn 才能 pickle）
# ---------------------------------------------------------------------------


def _worker_submit(payload: dict[str, Any]) -> dict[str, Any]:
    """在全新进程中开启独立事务提交一笔扫描。"""
    from app.database import SessionLocal
    from app.schemas import ScanRequest
    from app.service import PayloadConflictError, submit_scan

    session = SessionLocal()
    try:
        request = ScanRequest.model_validate(payload)
        try:
            response = submit_scan(session, request)
            session.commit()
            return {"ok": True, "status": 200, "body": response.model_dump(mode="json")}
        except PayloadConflictError as exc:
            session.rollback()
            return {
                "ok": True,
                "status": 409,
                "body": {"original_payload": exc.original_payload},
            }
        except Exception as exc:  # noqa: BLE001
            session.rollback()
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        session.close()


def _worker_count_if_first(payload: dict[str, Any], zone: str) -> dict[str, Any]:
    """模拟清点方：在 DB 屏障集合后同时冲线，仅赢得 first_seen 时清点 +1。

    插入 zone_tally 与腕带归属在**同一事务**中提交：
      * 获胜者 INSERT band 成功 -> 插入清点行 -> 提交
      * 落败者 ON CONFLICT 不插入 -> 不插清点 -> 提交 already_seen
    因此 zone_tally 的 (zone, band_id) 唯一约束最终行数 == 实际人数。
    """
    from sqlalchemy import text

    from app.database import SessionLocal
    from app.schemas import ScanRequest
    from app.service import submit_scan

    barrier_id = payload.pop("_barrier", None)
    expected = int(payload.pop("_racers", 2))

    session = SessionLocal()
    try:
        if barrier_id is not None:
            _meet_at_barrier(session, barrier_id, payload["gate_id"], expected)

        request = ScanRequest.model_validate(payload)
        response = submit_scan(session, request)
        outcome = response.result
        if outcome == "first_seen":
            session.execute(
                text(
                    "INSERT INTO zone_tally (zone, band_id, event_id) "
                    "VALUES (:zone, :band_id, :event_id)"
                ),
                {
                    "zone": zone,
                    "band_id": payload["band_id"],
                    "event_id": payload["event_id"],
                },
            )
        session.commit()
        return {"ok": True, "result": outcome, "counted": outcome == "first_seen"}
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        session.close()


def _worker_barrier_submit(barrier_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """先在 PostgreSQL 屏障上集合，再一起提交扫描，最大化竞争窗口。"""
    from app.database import SessionLocal
    from app.schemas import ScanRequest
    from app.service import submit_scan

    expected = int(payload.pop("_racers", 2))
    session = SessionLocal()
    try:
        _meet_at_barrier(session, barrier_id, payload["gate_id"], expected)
        request = ScanRequest.model_validate(payload)
        response = submit_scan(session, request)
        session.commit()
        return {"ok": True, "body": response.model_dump(mode="json")}
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        session.close()


def _meet_at_barrier(session, barrier_id: str, slot: str, expected: int) -> None:
    """所有参与者先在 race_barrier 登记，等到齐后再放行。"""
    from sqlalchemy import text

    session.execute(
        text(
            "INSERT INTO race_barrier(barrier_id, slot) VALUES (:b, :s) "
            "ON CONFLICT DO NOTHING"
        ),
        {"b": barrier_id, "s": slot},
    )
    session.commit()
    for _ in range(250):
        arrived = session.execute(
            text("SELECT COUNT(*) FROM race_barrier WHERE barrier_id = :b"),
            {"b": barrier_id},
        ).scalar_one()
        if arrived >= expected:
            break
        time.sleep(0.02)


def _create_roster_tx(session, payload: dict[str, Any]) -> dict[str, Any]:
    """在已打开的会话中创建名册并提交；409 时回滚，原名册不动。"""
    from app.schemas import RosterCreateRequest
    from app.service import RosterConflictError, create_roster

    request = RosterCreateRequest.model_validate(payload)
    try:
        response = create_roster(session, request)
        session.commit()
        return {"ok": True, "status": 201, "body": response.model_dump(mode="json")}
    except RosterConflictError:
        session.rollback()
        return {"ok": True, "status": 409, "body": {"roster_id": payload["roster_id"]}}


def _worker_create_roster(payload: dict[str, Any]) -> dict[str, Any]:
    """在全新进程中开启独立事务创建名册。"""
    from app.database import SessionLocal

    session = SessionLocal()
    try:
        return _create_roster_tx(session, payload)
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        session.close()


def _worker_barrier_create_roster(barrier_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """先在 PostgreSQL 屏障集合，再并发创建同一 roster_id，竞争唯一创建权。"""
    from app.database import SessionLocal

    expected = int(payload.pop("_racers", 2))
    slot = payload.pop("_slot", payload["roster_id"])
    session = SessionLocal()
    try:
        _meet_at_barrier(session, barrier_id, slot, expected)
        return _create_roster_tx(session, payload)
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        session.close()


def _worker_check_roster(roster_id: str) -> dict[str, Any]:
    """在全新进程中用独立连接核对名册（只读快照）。"""
    from app.database import SessionLocal
    from app.service import check_roster

    session = SessionLocal()
    try:
        result = check_roster(session, roster_id)
        if result is None:
            return {"ok": True, "status": 404}
        return {"ok": True, "status": 200, "body": result.model_dump(mode="json")}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        session.close()


def _submit_inspection_tx(session, payload: dict[str, Any]) -> dict[str, Any]:
    """在已打开的会话中提交巡检并提交事务；409 时回滚，原记录不动。"""
    from app.schemas import InspectionRequest
    from app.service import InspectionConflictError, submit_inspection

    request = InspectionRequest.model_validate(payload)
    try:
        response = submit_inspection(session, request)
        session.commit()
        return {"ok": True, "status": 201, "body": response.model_dump(mode="json")}
    except InspectionConflictError:
        session.rollback()
        return {
            "ok": True,
            "status": 409,
            "body": {"inspection_id": payload["inspection_id"]},
        }


def _worker_submit_inspection(payload: dict[str, Any]) -> dict[str, Any]:
    """在全新进程中开启独立事务提交一条巡检记录。"""
    from app.database import SessionLocal

    session = SessionLocal()
    try:
        return _submit_inspection_tx(session, payload)
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        session.close()


def _worker_barrier_submit_inspection(
    barrier_id: str, payload: dict[str, Any]
) -> dict[str, Any]:
    """先在 PostgreSQL 屏障集合，再并发提交巡检，竞争同一 inspection_id。"""
    from app.database import SessionLocal

    expected = int(payload.pop("_racers", 2))
    slot = payload.pop("_slot", payload["inspection_id"])
    session = SessionLocal()
    try:
        _meet_at_barrier(session, barrier_id, slot, expected)
        return _submit_inspection_tx(session, payload)
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        session.close()


def _worker_latest_inspection(gate_id: str) -> dict[str, Any]:
    """在全新进程中用独立连接查询某闸机的最近一次巡检（只读快照）。"""
    from app.database import SessionLocal
    from app.service import get_latest_inspection

    session = SessionLocal()
    try:
        result = get_latest_inspection(session, gate_id)
        if result is None:
            return {"ok": True, "status": 404}
        return {"ok": True, "status": 200, "body": result.model_dump(mode="json")}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        session.close()


# ---------------------------------------------------------------------------
# spawn 可用的顶层分发器
# ---------------------------------------------------------------------------


def _dispatch_submit(queue, payload: dict[str, Any]) -> None:
    queue.put(_worker_submit(payload))


def _dispatch_count(queue, payload: dict[str, Any], zone: str) -> None:
    queue.put(_worker_count_if_first(payload, zone))


def _dispatch_barrier(queue, barrier_id: str, payload: dict[str, Any]) -> None:
    queue.put(_worker_barrier_submit(barrier_id, payload))


def _dispatch_barrier_create_roster(
    queue, barrier_id: str, payload: dict[str, Any]
) -> None:
    queue.put(_worker_barrier_create_roster(barrier_id, payload))


def _dispatch_barrier_submit_inspection(
    queue, barrier_id: str, payload: dict[str, Any]
) -> None:
    queue.put(_worker_barrier_submit_inspection(barrier_id, payload))


def _dispatch_named(queue, fn_name: str, arguments: list[Any]) -> None:
    fn = globals()[fn_name]
    try:
        queue.put(fn(*arguments))
    except Exception as exc:  # noqa: BLE001
        queue.put({"ok": False, "error": f"{type(exc).__name__}: {exc}"})


# ---------------------------------------------------------------------------
# 父进程侧编排
# ---------------------------------------------------------------------------


def _spawn_run(target, args: tuple, timeout: int = 60) -> dict[str, Any]:
    ctx = get_context("spawn")
    queue = ctx.Queue()
    proc = ctx.Process(target=target, args=(queue, *args))
    proc.start()
    proc.join(timeout=timeout)
    if proc.is_alive():
        proc.kill()
        proc.join()
        return {"ok": False, "error": "worker timed out"}
    if proc.exitcode != 0:
        return {"ok": False, "error": f"worker exited with code {proc.exitcode}"}
    try:
        result = queue.get(timeout=10)
    except Exception as exc:  # noqa: BLE001
        result = {"ok": False, "error": f"worker result lost: {exc}"}
    queue.close()
    return result


def _sparm_map(target, arg_list: list[tuple], timeout: int = 60) -> list[dict[str, Any]]:
    ctx = get_context("spawn")
    queue = ctx.Queue()
    procs = [ctx.Process(target=target, args=(queue, *args)) for args in arg_list]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(timeout=timeout)
        if proc.is_alive():
            proc.kill()
            proc.join()

    results: list[dict[str, Any]] = []
    for _ in procs:
        try:
            results.append(queue.get(timeout=10))
        except Exception:  # noqa: BLE001
            results.append({"ok": False, "error": "worker result lost"})
    queue.close()
    return results


def run_in_process(fn_name: str, *args) -> dict[str, Any]:
    """在独立 spawn 进程中按名字运行本模块的顶层函数。"""
    return _spawn_run(_dispatch_named, (fn_name, list(args)))


def submit_concurrently(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """多笔扫描在各自独立进程中同时执行。"""
    return _sparm_map(_dispatch_submit, [(p,) for p in payloads])


def count_concurrently(payloads: list[dict[str, Any]], zone: str) -> list[dict[str, Any]]:
    """多个清点进程在数据库屏障集合后并发提交，竞争同一条腕带事实。"""
    return _sparm_map(_dispatch_count, [(p, zone) for p in payloads])


def barrier_submit_concurrently(
    barrier_id: str, payloads: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    return _sparm_map(
        _dispatch_barrier, [(barrier_id, p) for p in payloads]
    )


def create_roster_concurrently(
    barrier_id: str, payloads: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """多个独立进程在数据库屏障集合后并发创建名册，竞争同一 roster_id。"""
    return _sparm_map(
        _dispatch_barrier_create_roster, [(barrier_id, p) for p in payloads]
    )


def submit_inspection_concurrently(
    barrier_id: str, payloads: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """多个独立进程在数据库屏障集合后并发提交巡检记录。"""
    return _sparm_map(
        _dispatch_barrier_submit_inspection, [(barrier_id, p) for p in payloads]
    )


def result_sanity(result: dict[str, Any]) -> None:
    assert result.get("ok"), json.dumps(result, ensure_ascii=False)

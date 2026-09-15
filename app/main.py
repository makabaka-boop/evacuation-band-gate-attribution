"""FastAPI 入口。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from .database import engine, get_session
from .models import Base
from .request_context import RequestIdMiddleware, install_request_id_logging
from .schemas import (
    BandFact,
    DeploymentArrivalRequest,
    DeploymentCreateRequest,
    DeploymentResponse,
    GateInspectionStatus,
    InspectionRequest,
    InspectionResponse,
    RosterCheckResponse,
    RosterCreateRequest,
    RosterCreatedResponse,
    ScanRequest,
    ScanResponse,
)
from .service import (
    ArrivalConflictError,
    DeploymentConflictError,
    InspectionConflictError,
    PayloadConflictError,
    RosterConflictError,
    check_roster,
    confirm_arrival,
    create_deployment,
    create_roster,
    get_band_fact,
    get_deployment,
    get_latest_inspection,
    submit_inspection,
    submit_scan,
)

# 应用日志（app.*）统一附加 request_id，便于按标识串起一次调用的
# 入口、业务处理与数据库会话日志。
install_request_id_logging()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 幂等的建表；正式环境也可改用 Alembic，此处保持自包含。
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="火警疏散腕带首次通过 API",
    version="1.3.0",
    description="以数据库唯一约束与事务保证同一腕带全局恰有一个 first_seen；"
    "疏散名册复用该事实核对尚未过闸人员；闸机巡检记录按追加式持久化，"
    "供演练前确认闸机可用状态；增援派驻记录以数据库条件更新裁决到岗确认，"
    "阶段只沿待到岗到已到岗单向推进。",
    lifespan=lifespan,
)

# 入口中间件：所有请求解析/生成 X-Request-ID 并回写响应头（含各类错误
# 响应）；非法标识在进入路由与创建数据库会话之前即以 400 拒绝。
app.add_middleware(RequestIdMiddleware)


@app.exception_handler(PayloadConflictError)
def _payload_conflict_handler(_request: Request, exc: PayloadConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "detail": "event_id was already used with a different payload",
            "original_payload": exc.original_payload,
        },
    )


@app.exception_handler(RosterConflictError)
def _roster_conflict_handler(_request: Request, exc: RosterConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "detail": "roster_id already exists",
            "roster_id": exc.roster_id,
        },
    )


@app.exception_handler(InspectionConflictError)
def _inspection_conflict_handler(
    _request: Request, exc: InspectionConflictError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "detail": "inspection_id already exists",
            "inspection_id": exc.inspection_id,
        },
    )


@app.exception_handler(DeploymentConflictError)
def _deployment_conflict_handler(
    _request: Request, exc: DeploymentConflictError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "detail": "deployment_id already exists",
            "deployment_id": exc.deployment_id,
        },
    )


@app.exception_handler(ArrivalConflictError)
def _arrival_conflict_handler(
    _request: Request, exc: ArrivalConflictError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "detail": "deployment already confirmed",
            "deployment_id": exc.deployment_id,
            # 回带先到岗时间，便于调度员核对；原值未被本次请求覆盖。
            "arrived_at": exc.arrived_at.isoformat(),
        },
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/scans", response_model=ScanResponse)
def create_scan(
    payload: ScanRequest,
    session: Session = Depends(get_session),
) -> ScanResponse:
    try:
        response = submit_scan(session, payload)
        session.commit()
    except PayloadConflictError:
        session.rollback()
        raise
    except Exception:
        session.rollback()
        raise
    return response


@app.get("/bands/{band_id}", response_model=BandFact)
def read_band(band_id: str, session: Session = Depends(get_session)) -> BandFact:
    fact = get_band_fact(session, band_id)
    if fact is None:
        raise HTTPException(status_code=404, detail="band has no recorded scan")
    return BandFact(
        band_id=fact.band_id,
        event_id=fact.event_id,
        gate_id=fact.gate_id,
        scanned_at=fact.scanned_at,
        created_at=fact.created_at,
    )


@app.post(
    "/rosters",
    response_model=RosterCreatedResponse,
    status_code=status.HTTP_201_CREATED,
)
def post_roster(
    payload: RosterCreateRequest,
    session: Session = Depends(get_session),
) -> RosterCreatedResponse:
    try:
        response = create_roster(session, payload)
        session.commit()
    except RosterConflictError:
        session.rollback()
        raise
    except Exception:
        session.rollback()
        raise
    return response


@app.get("/rosters/{roster_id:path}", response_model=RosterCheckResponse)
def get_roster_check(
    roster_id: str, session: Session = Depends(get_session)
) -> RosterCheckResponse:
    # 用 :path 转换器接收任意 roster_id（含 "/" 的层级式标识也按创建时的
    # 原标识逐字寻址），保证凡已创建的名册都可按原标识核对；不存在仍 404。
    result = check_roster(session, roster_id)
    if result is None:
        raise HTTPException(status_code=404, detail="roster not found")
    return result


@app.post(
    "/inspections",
    response_model=InspectionResponse,
    status_code=status.HTTP_201_CREATED,
)
def post_inspection(
    payload: InspectionRequest,
    session: Session = Depends(get_session),
) -> InspectionResponse:
    try:
        response = submit_inspection(session, payload)
        session.commit()
    except InspectionConflictError:
        session.rollback()
        raise
    except Exception:
        session.rollback()
        raise
    return response


@app.get(
    "/gates/{gate_id}/inspections/latest",
    response_model=GateInspectionStatus,
)
def get_gate_inspection_latest(
    gate_id: str, session: Session = Depends(get_session)
) -> GateInspectionStatus:
    result = get_latest_inspection(session, gate_id)
    if result is None:
        raise HTTPException(status_code=404, detail="gate has no inspection record")
    return result


@app.post(
    "/deployments",
    response_model=DeploymentResponse,
    status_code=status.HTTP_201_CREATED,
)
def post_deployment(
    payload: DeploymentCreateRequest,
    session: Session = Depends(get_session),
) -> DeploymentResponse:
    try:
        response = create_deployment(session, payload)
        session.commit()
    except DeploymentConflictError:
        session.rollback()
        raise
    except Exception:
        session.rollback()
        raise
    return response


@app.post(
    "/deployments/{deployment_id:path}/arrival",
    response_model=DeploymentResponse,
)
def post_deployment_arrival(
    deployment_id: str,
    payload: DeploymentArrivalRequest,
    session: Session = Depends(get_session),
) -> DeploymentResponse:
    # 用 :path 转换器接收任意 deployment_id（含 "/" 的层级式标识也按创建时
    # 的原标识逐字寻址）；未知派驻 404，重复确认由条件更新裁决为 409。
    try:
        response = confirm_arrival(session, deployment_id, payload)
        if response is None:
            raise HTTPException(status_code=404, detail="deployment not found")
        session.commit()
    except ArrivalConflictError:
        session.rollback()
        raise
    except HTTPException:
        session.rollback()
        raise
    except Exception:
        session.rollback()
        raise
    return response


@app.get("/deployments/{deployment_id:path}", response_model=DeploymentResponse)
def get_deployment_fact(
    deployment_id: str, session: Session = Depends(get_session)
) -> DeploymentResponse:
    result = get_deployment(session, deployment_id)
    if result is None:
        raise HTTPException(status_code=404, detail="deployment not found")
    return result

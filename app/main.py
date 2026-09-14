"""FastAPI 入口。"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from .database import engine, get_session
from .models import Base
from .schemas import (
    BandFact,
    RosterCheckResponse,
    RosterCreateRequest,
    RosterCreatedResponse,
    ScanRequest,
    ScanResponse,
)
from .service import (
    PayloadConflictError,
    RosterConflictError,
    check_roster,
    create_roster,
    get_band_fact,
    submit_scan,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 幂等的建表；正式环境也可改用 Alembic，此处保持自包含。
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="火警疏散腕带首次通过 API",
    version="1.1.0",
    description="以数据库唯一约束与事务保证同一腕带全局恰有一个 first_seen；"
    "疏散名册复用该事实核对尚未过闸人员。",
    lifespan=lifespan,
)


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


@app.get("/rosters/{roster_id}", response_model=RosterCheckResponse)
def get_roster_check(
    roster_id: str, session: Session = Depends(get_session)
) -> RosterCheckResponse:
    result = check_roster(session, roster_id)
    if result is None:
        raise HTTPException(status_code=404, detail="roster not found")
    return result

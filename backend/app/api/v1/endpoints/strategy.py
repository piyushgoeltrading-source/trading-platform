"""
app/api/strategy.py

Strategy API Router — PiyushTrade
====================================
Endpoints:
  POST   /strategies          — create a strategy
  GET    /strategies          — list user's strategies (paginated)
  GET    /strategies/{id}     — get single strategy
  PUT    /strategies/{id}     — update strategy
  DELETE /strategies/{id}     — soft-delete (sets status=ARCHIVED)

Rules:
  - All endpoints require JWT authentication.
  - user_id is always extracted from the JWT token — never from request body.
  - Users can only access their own strategies (enforced on every query).
  - Soft delete only — strategies are never hard-deleted (audit trail).
  - All errors follow the standard PiyushTrade error envelope.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.core.logging import get_structured_logger
from app.core.time_utils import now_utc
from app.models.strategy import Strategy, StrategyStatus
from app.models.user import User
from app.schemas.strategy_schema import (
    StrategyCreate,
    StrategyListResponse,
    StrategyResponse,
    StrategyUpdate,
)

logger = get_structured_logger(__name__)

router = APIRouter(prefix="/strategies", tags=["strategies"])


# ---------------------------------------------------------------------------
# Helper: ownership check
# ---------------------------------------------------------------------------

async def _get_strategy_or_404(
    strategy_id: int,
    user_id: int,
    db: AsyncSession,
) -> Strategy:
    """
    Fetch a strategy by ID, ensuring it belongs to the requesting user.
    Raises 404 if not found or not owned by the user.
    This prevents both not-found and unauthorized-access leaking strategy existence.
    """
    result = await db.execute(
        select(Strategy).where(
            Strategy.id == strategy_id,
            Strategy.user_id == user_id,
        )
    )
    strategy = result.scalar_one_or_none()
    if strategy is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error_code": "STRATEGY_NOT_FOUND",
                "message": f"Strategy {strategy_id} not found",
                "details": {"strategy_id": strategy_id},
            },
        )
    return strategy


# ---------------------------------------------------------------------------
# POST /strategies
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=StrategyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new strategy",
)
async def create_strategy(
    payload: StrategyCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> StrategyResponse:
    """
    Create a new strategy for the authenticated user.
    Status defaults to DRAFT on creation.
    """
    strategy = Strategy(
        user_id=current_user.id,
        name=payload.name,
        description=payload.description,
        instrument=payload.instrument,
        parameters=payload.parameters,
        status=StrategyStatus.draft,
    )

    db.add(strategy)
    await db.commit()
    await db.refresh(strategy)

    logger.info(
        "Strategy created",
        extra={
            "event": "strategy_created",
            "user_id": current_user.id,
            "strategy_id": strategy.id,
            "instrument": strategy.instrument,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return StrategyResponse.model_validate(strategy)


# ---------------------------------------------------------------------------
# GET /strategies
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=StrategyListResponse,
    summary="List strategies for the authenticated user",
)
async def list_strategies(
    skip: int = Query(default=0, ge=0, description="Pagination offset"),
    limit: int = Query(default=20, ge=1, le=100, description="Page size"),
    status_filter: StrategyStatus | None = Query(
        default=None,
        alias="status",
        description="Filter by strategy status",
    ),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> StrategyListResponse:
    """
    Return a paginated list of strategies owned by the authenticated user.
    Optionally filter by status.
    """
    base_query = select(Strategy).where(Strategy.user_id == current_user.id)

    if status_filter is not None:
        base_query = base_query.where(Strategy.status == status_filter)

    # Total count
    count_result = await db.execute(
        select(func.count()).select_from(base_query.subquery())
    )
    total = count_result.scalar_one()

    # Paginated results — newest first
    result = await db.execute(
        base_query.order_by(Strategy.created_at.desc()).offset(skip).limit(limit)
    )
    strategies = result.scalars().all()

    return StrategyListResponse(
        total=total,
        items=[StrategyResponse.model_validate(s) for s in strategies],
    )


# ---------------------------------------------------------------------------
# GET /strategies/{strategy_id}
# ---------------------------------------------------------------------------

@router.get(
    "/{strategy_id}",
    response_model=StrategyResponse,
    summary="Get a single strategy",
)
async def get_strategy(
    strategy_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> StrategyResponse:
    """Return a single strategy by ID. User must own the strategy."""
    strategy = await _get_strategy_or_404(strategy_id, current_user.id, db)
    return StrategyResponse.model_validate(strategy)


# ---------------------------------------------------------------------------
# PUT /strategies/{strategy_id}
# ---------------------------------------------------------------------------

# Valid status transitions — enforced to prevent illegal state changes
_ALLOWED_TRANSITIONS: dict[StrategyStatus, set[StrategyStatus]] = {
    StrategyStatus.draft:    {StrategyStatus.active, StrategyStatus.archived},
    StrategyStatus.active:   {StrategyStatus.paused, StrategyStatus.archived},
    StrategyStatus.paused:   {StrategyStatus.active, StrategyStatus.archived},
    StrategyStatus.archived: set(),  # Terminal state — no transitions out
}


@router.put(
    "/{strategy_id}",
    response_model=StrategyResponse,
    summary="Update a strategy",
)
async def update_strategy(
    strategy_id: int,
    payload: StrategyUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> StrategyResponse:
    """
    Update a strategy. Only provided fields are changed.
    Status transitions are validated against the allowed transition map.
    ARCHIVED strategies cannot be modified.
    """
    strategy = await _get_strategy_or_404(strategy_id, current_user.id, db)

    # Block edits to archived strategies
    if strategy.status == StrategyStatus.archived:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error_code": "STRATEGY_ARCHIVED",
                "message": "Archived strategies cannot be modified",
                "details": {"strategy_id": strategy_id},
            },
        )

    # Validate status transition
    if payload.status is not None:
        current_status = StrategyStatus(strategy.status)
        requested_status = StrategyStatus(payload.status)
        if requested_status not in _ALLOWED_TRANSITIONS.get(current_status, set()):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "error_code": "INVALID_STATUS_TRANSITION",
                    "message": (
                        f"Cannot transition from {current_status.value} "
                        f"to {requested_status.value}"
                    ),
                    "details": {
                        "current_status": current_status.value,
                        "requested_status": requested_status.value,
                        "allowed": [s.value for s in _ALLOWED_TRANSITIONS[current_status]],
                    },
                },
            )
        strategy.status = payload.status

    # Apply field updates
    if payload.name is not None:
        strategy.name = payload.name
    if payload.description is not None:
        strategy.description = payload.description
    if payload.parameters is not None:
        strategy.parameters = payload.parameters

    await db.commit()
    await db.refresh(strategy)

    logger.info(
        "Strategy updated",
        extra={
            "event": "strategy_updated",
            "user_id": current_user.id,
            "strategy_id": strategy.id,
            "timestamp_utc": now_utc().isoformat(),
        },
    )

    return StrategyResponse.model_validate(strategy)


# ---------------------------------------------------------------------------
# DELETE /strategies/{strategy_id}
# ---------------------------------------------------------------------------

@router.delete(
    "/{strategy_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Archive (soft-delete) a strategy",
)
async def delete_strategy(
    strategy_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Soft-delete a strategy by setting its status to ARCHIVED.
    Strategies are never hard-deleted — audit trail must be preserved.
    Archiving an already-archived strategy is a no-op (idempotent).
    """
    strategy = await _get_strategy_or_404(strategy_id, current_user.id, db)

    if strategy.status != StrategyStatus.archived:
        strategy.status = StrategyStatus.archived
        await db.commit()

        logger.info(
            "Strategy archived",
            extra={
                "event": "strategy_archived",
                "user_id": current_user.id,
                "strategy_id": strategy.id,
                "timestamp_utc": now_utc().isoformat(),
            },
        )

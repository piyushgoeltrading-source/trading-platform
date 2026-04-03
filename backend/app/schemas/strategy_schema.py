"""
app/schemas/strategy_schema.py

Pydantic Schemas — Strategy
=============================
Used for:
  - Request validation (StrategyCreate, StrategyUpdate)
  - Response serialization (StrategyResponse)

Rules:
  - user_id is NEVER accepted from the request body.
    It is always injected from the JWT token in the API layer.
  - All datetime fields are returned as UTC ISO 8601 strings.
  - Enums are serialized as their string values (use_enum_values=True).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from app.models.strategy import Instrument, StrategyStatus


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------

class StrategyCreate(BaseModel):
    """
    Payload accepted when creating a new strategy.
    user_id is injected from JWT — never from this schema.
    """

    name: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Human-readable strategy name",
    )
    description: str | None = Field(
        default=None,
        max_length=1024,
        description="Optional strategy description",
    )
    instrument: Instrument = Field(
        ...,
        description="Underlying instrument: NIFTY or BANKNIFTY",
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Strategy-specific parameters stored as JSON",
    )

    @field_validator("name")
    @classmethod
    def name_must_not_be_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("name must not be blank or whitespace only")
        return v.strip()

    @field_validator("parameters")
    @classmethod
    def parameters_must_be_serializable(cls, v: dict) -> dict:
        """Ensure parameters can be serialized to JSON (no non-serializable types)."""
        import json
        try:
            json.dumps(v)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"parameters must be JSON-serializable: {exc}") from exc
        return v

    @field_validator("parameters")
    @classmethod
    def parameters_business_rules(cls, v: dict[str, Any]) -> dict[str, Any]:
        """
        Business validation for strategy parameters.

        Enforces:
        - lots must be a positive integer (if provided)
        """
        lots = v.get("lots")
        if lots is not None:
            if not isinstance(lots, int) or lots <= 0:
                raise ValueError("parameters.lots must be a positive integer")
        return v

    model_config = {"use_enum_values": True}


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

class StrategyUpdate(BaseModel):
    """
    Payload accepted when updating an existing strategy.
    All fields are optional — only provided fields are updated.
    Status transitions are validated.
    """

    name: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
    )
    description: str | None = Field(
        default=None,
        max_length=1024,
    )
    parameters: dict[str, Any] | None = Field(default=None)
    status: StrategyStatus | None = Field(
        default=None,
        description="Request a status transition",
    )

    @field_validator("name")
    @classmethod
    def name_must_not_be_blank(cls, v: str | None) -> str | None:
        if v is not None and not v.strip():
            raise ValueError("name must not be blank or whitespace only")
        return v.strip() if v else v

    @field_validator("parameters")
    @classmethod
    def parameters_must_be_serializable(cls, v: dict | None) -> dict | None:
        if v is None:
            return v
        import json
        try:
            json.dumps(v)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"parameters must be JSON-serializable: {exc}") from exc
        return v

    @model_validator(mode="after")
    def at_least_one_field(self) -> "StrategyUpdate":
        if all(
            getattr(self, f) is None
            for f in ("name", "description", "parameters", "status")
        ):
            raise ValueError("At least one field must be provided for update")
        return self

    model_config = {"use_enum_values": True}


# ---------------------------------------------------------------------------
# Response
# ---------------------------------------------------------------------------

class StrategyResponse(BaseModel):
    """
    Serialized strategy returned by the API.
    All datetimes are UTC ISO 8601.
    """

    id: int
    user_id: int
    name: str
    description: str | None
    instrument: str           # serialized enum value
    status: str               # serialized enum value
    parameters: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True,   # allows orm_mode / SQLAlchemy model → schema
        "use_enum_values": True,
    }


# ---------------------------------------------------------------------------
# List response wrapper
# ---------------------------------------------------------------------------

class StrategyListResponse(BaseModel):
    """Paginated list of strategies."""

    total: int
    items: list[StrategyResponse]
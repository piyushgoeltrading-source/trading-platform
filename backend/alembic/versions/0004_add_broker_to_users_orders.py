"""add broker to users and orders

Revision ID: 0004
Revises: fad3f69cfcc0
Create Date: 2026-03-31

Adds:
  - brokername enum type (zerodha, nuvama)
  - users.broker  VARCHAR NOT NULL DEFAULT 'zerodha'
  - orders.broker VARCHAR NOT NULL DEFAULT 'zerodha'

Rules followed:
  - PgEnum.create(bind, checkfirst=True) BEFORE op.add_column — avoids
    SQLAlchemy double-firing CREATE TYPE.
  - create_type=False in column definitions — type already exists.
  - Default 'zerodha' for all existing rows — safe migration of live data.
  - downgrade() drops columns first, then the enum type.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ENUM as PgEnum

revision = "0004"
down_revision = "fad3f69cfcc0"
branch_labels = None
depends_on = None

# Define once — reused in upgrade() and downgrade()
brokername = PgEnum("zerodha", "nuvama", name="brokername", create_type=True)


def upgrade() -> None:
    bind = op.get_bind()

    # ------------------------------------------------------------------
    # 1. Create enum type explicitly BEFORE any column operations.
    #    checkfirst=True makes it safe to run twice (idempotent).
    # ------------------------------------------------------------------
    brokername.create(bind, checkfirst=True)

    # ------------------------------------------------------------------
    # 2. Add broker column to users.
    #    Default 'zerodha' — all existing users are set to Zerodha.
    #    NOT NULL enforced after backfill.
    # ------------------------------------------------------------------
    op.add_column(
        "users",
        sa.Column(
            "broker",
            PgEnum("zerodha", "nuvama", name="brokername", create_type=False),
            nullable=True,          # temporarily nullable for the backfill
        ),
    )
    op.execute("UPDATE users SET broker = 'zerodha' WHERE broker IS NULL")
    op.alter_column("users", "broker", nullable=False)

    # ------------------------------------------------------------------
    # 3. Add broker column to orders.
    #    Every order must record which broker it was placed through.
    #    Existing orders backfilled to 'zerodha'.
    # ------------------------------------------------------------------
    op.add_column(
        "orders",
        sa.Column(
            "broker",
            PgEnum("zerodha", "nuvama", name="brokername", create_type=False),
            nullable=True,          # temporarily nullable for the backfill
        ),
    )
    op.execute("UPDATE orders SET broker = 'zerodha' WHERE broker IS NULL")
    op.alter_column("orders", "broker", nullable=False)


def downgrade() -> None:
    bind = op.get_bind()

    # Drop columns first, then the enum type they reference
    op.drop_column("orders", "broker")
    op.drop_column("users", "broker")
    brokername.drop(bind, checkfirst=True)

"""add role strategy order trade

Revision ID: 0002
Revises: 0001_initial_users
Create Date: 2026-03-01

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ENUM as PgEnum

# revision identifiers
revision = "0002"
down_revision = "0001_initial_users"
branch_labels = None
depends_on = None

# Define all enum types once, with checkfirst=True so they are never
# created twice regardless of what SQLAlchemy does internally.
userrole     = PgEnum("admin", "user",                              name="userrole",      create_type=True)
instrument   = PgEnum("NIFTY", "BANKNIFTY", "SENSEX", "BANKEX",    name="instrument",    create_type=True)
strategystatus = PgEnum("draft", "active", "paused", "archived",   name="strategystatus",create_type=True)
orderstatus  = PgEnum("PENDING","SENT","FILLED","FAILED","CANCELLED",name="orderstatus",  create_type=True)
orderside    = PgEnum("BUY", "SELL",                                name="orderside",     create_type=True)


def upgrade() -> None:
    bind = op.get_bind()

    # ------------------------------------------------------------------
    # 1. Create all enum types explicitly with checkfirst=True.
    #    This runs before op.create_table so SQLAlchemy finds the types
    #    already exist and skips its own CREATE TYPE calls.
    # ------------------------------------------------------------------
    userrole.create(bind, checkfirst=True)
    instrument.create(bind, checkfirst=True)
    strategystatus.create(bind, checkfirst=True)
    orderstatus.create(bind, checkfirst=True)
    orderside.create(bind, checkfirst=True)

    # ------------------------------------------------------------------
    # 2. Alter users.role VARCHAR → userrole enum.
    # ------------------------------------------------------------------
    op.execute("UPDATE users SET role = 'user' WHERE role NOT IN ('admin', 'user')")
    op.execute("ALTER TABLE users ALTER COLUMN role DROP DEFAULT")
    op.execute("ALTER TABLE users ALTER COLUMN role TYPE userrole USING role::userrole")
    op.execute("ALTER TABLE users ALTER COLUMN role SET DEFAULT 'user'::userrole")
    op.execute("ALTER TABLE users ALTER COLUMN role SET NOT NULL")

    # ------------------------------------------------------------------
    # 3. Create strategies table — reference the already-created enums.
    # ------------------------------------------------------------------
    op.create_table(
        "strategies",
        sa.Column("id", sa.Integer(), primary_key=True, index=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("instrument", PgEnum("NIFTY", "BANKNIFTY", "SENSEX", "BANKEX", name="instrument", create_type=False), nullable=False),
        sa.Column("legs", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("status", PgEnum("draft", "active", "paused", "archived", name="strategystatus", create_type=False), nullable=False, server_default="draft"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # ------------------------------------------------------------------
    # 4. Create orders table.
    # ------------------------------------------------------------------
    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), primary_key=True, index=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("strategy_id", sa.Integer(), sa.ForeignKey("strategies.id", ondelete="SET NULL"), nullable=True, index=True),
        sa.Column("idempotency_key", sa.String(), nullable=False, unique=True),
        sa.Column("status", PgEnum("PENDING","SENT","FILLED","FAILED","CANCELLED", name="orderstatus", create_type=False), nullable=False, server_default="PENDING"),
        sa.Column("strike", sa.Numeric(10, 2), nullable=False),
        sa.Column("expiry", sa.Date(), nullable=False),
        sa.Column("side", PgEnum("BUY", "SELL", name="orderside", create_type=False), nullable=False),
        sa.Column("qty", sa.Integer(), nullable=False),
        sa.Column("instrument", sa.String(), nullable=False),
        sa.Column("broker_order_id", sa.String(), nullable=True),
        sa.Column("rejection_reason", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_orders_idempotency_key", "orders", ["idempotency_key"], unique=True)

    # ------------------------------------------------------------------
    # 5. Create trades table.
    # ------------------------------------------------------------------
    op.create_table(
        "trades",
        sa.Column("id", sa.Integer(), primary_key=True, index=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("order_id", sa.Integer(), sa.ForeignKey("orders.id", ondelete="RESTRICT"), nullable=False, unique=True, index=True),
        sa.Column("fill_price", sa.Numeric(10, 2), nullable=False),
        sa.Column("fill_qty", sa.Integer(), nullable=False),
        sa.Column("realised_pnl", sa.Numeric(12, 2), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("exchange_trade_id", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    bind = op.get_bind()

    op.drop_table("trades")
    op.drop_index("ix_orders_idempotency_key", table_name="orders")
    op.drop_table("orders")
    op.drop_table("strategies")

    # Revert users.role back to VARCHAR
    op.execute("ALTER TABLE users ALTER COLUMN role DROP DEFAULT")
    op.execute("ALTER TABLE users ALTER COLUMN role TYPE VARCHAR USING role::VARCHAR")
    op.execute("ALTER TABLE users ALTER COLUMN role SET DEFAULT 'user'")

    # Drop enum types in reverse dependency order
    orderside.drop(bind, checkfirst=True)
    orderstatus.drop(bind, checkfirst=True)
    strategystatus.drop(bind, checkfirst=True)
    instrument.drop(bind, checkfirst=True)
    userrole.drop(bind, checkfirst=True)

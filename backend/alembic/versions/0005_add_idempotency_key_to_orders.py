# alembic/0005_add_idempotency_key_to_orders.py
def upgrade():
    op.add_column(
        'orders',
        sa.Column('idempotency_key', sa.String(64), nullable=True, index=True)
    )
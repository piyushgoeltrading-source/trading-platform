"""add description and parameters to strategy

Revision ID: fad3f69cfcc0
Revises: 0002
Create Date: 2026-03-29 21:57:56.770926

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


revision: str = 'fad3f69cfcc0'
down_revision: Union[str, None] = '0002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("strategies", sa.Column("description", sa.String(), nullable=True))
    op.add_column("strategies", sa.Column("parameters", sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")))


def downgrade() -> None:
    op.drop_column("strategies", "parameters")
    op.drop_column("strategies", "description")
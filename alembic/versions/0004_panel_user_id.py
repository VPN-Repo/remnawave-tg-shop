"""panel user numeric id (Remnawave 3.x)

Revision ID: 0004_panel_user_id
Revises: 0003_promo_curr_act_not_null
Create Date: 2026-10-25 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0004_panel_user_id"
down_revision: Union[str, Sequence[str], None] = "0003_promo_curr_act_not_null"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("panel_user_id", sa.BigInteger(), nullable=True))
    op.create_index("ix_users_panel_user_id", "users", ["panel_user_id"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_users_panel_user_id", table_name="users")
    op.drop_column("users", "panel_user_id")

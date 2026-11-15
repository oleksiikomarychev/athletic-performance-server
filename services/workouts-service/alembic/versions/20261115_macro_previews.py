"""Подтверждения макросов и результаты их исполнения."""

import sqlalchemy as sa
from alembic import op

revision = "20261115_macro_previews"
down_revision = "21dd18c3fc62"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "macro_previews",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("user_id", sa.String(255), nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=False),
        sa.Column("effects", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_table("macro_previews")

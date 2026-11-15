"""Однократное срабатывание правила на наблюдение и целевую тренировку."""

import sqlalchemy as sa
from alembic import op

revision = "20261115_macro_executions"
down_revision = "20261115_macro_previews"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "macro_executions",
        sa.Column("user_id", sa.String(255), primary_key=True),
        sa.Column("plan_id", sa.Integer(), primary_key=True),
        sa.Column("workout_id", sa.Integer(), primary_key=True),
        sa.Column("rule_key", sa.String(128), primary_key=True),
    )


def downgrade():
    op.drop_table("macro_executions")

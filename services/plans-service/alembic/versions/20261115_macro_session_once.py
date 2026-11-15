"""Однократное продвижение плана по завершённой тренировке."""

import sqlalchemy as sa
from alembic import op

revision = "20261115_macro_session_once"
down_revision = "20261115_macro_weights"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "applied_plan_workouts", sa.Column("session_advanced", sa.Boolean(), nullable=False, server_default=sa.false())
    )


def downgrade():
    op.drop_column("applied_plan_workouts", "session_advanced")

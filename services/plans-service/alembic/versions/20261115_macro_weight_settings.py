"""Снимок настроек нагрузки тренировки применённого плана."""

import sqlalchemy as sa
from alembic import op

revision = "20261115_macro_weights"
down_revision = "20261110_plan_catalog"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("applied_plan_workouts", sa.Column("macro_weight_settings", sa.JSON(), nullable=True))


def downgrade():
    op.drop_column("applied_plan_workouts", "macro_weight_settings")

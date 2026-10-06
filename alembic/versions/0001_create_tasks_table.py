"""create tasks table

Revision ID: 0001
Revises:
Create Date: 2024-01-01 00:00:00.000000

Migration escrita a mao (nao autogerada): cria o tipo enum task_status, a tabela
tasks e os indices de consulta.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# identificadores usados pelo Alembic
revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TASK_STATUS_VALUES = ("PENDING", "PROCESSING", "COMPLETED", "FAILED")

# create_type=False: o tipo e criado/removido explicitamente abaixo, para que
# `alembic upgrade head --sql` (modo offline) tambem gere o DDL completo.
task_status = postgresql.ENUM(
    *TASK_STATUS_VALUES,
    name="task_status",
    create_type=False,
)


def upgrade() -> None:
    op.execute(
        "CREATE TYPE task_status AS ENUM ("
        + ", ".join(f"'{value}'" for value in TASK_STATUS_VALUES)
        + ")"
    )

    op.create_table(
        "tasks",
        sa.Column("task_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", task_status, nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("task_id", name="pk_tasks"),
    )

    op.create_index("ix_tasks_status", "tasks", ["status"], unique=False)
    op.create_index("ix_tasks_event_type", "tasks", ["event_type"], unique=False)
    op.create_index("ix_tasks_created_at", "tasks", ["created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_tasks_created_at", table_name="tasks")
    op.drop_index("ix_tasks_event_type", table_name="tasks")
    op.drop_index("ix_tasks_status", table_name="tasks")
    op.drop_table("tasks")
    op.execute("DROP TYPE task_status")

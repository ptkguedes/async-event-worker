"""add claimed_at to tasks

Revision ID: 0002
Revises: 0001
Create Date: 2024-01-02 00:00:00.000000

Coluna de LEASE do claim idempotente: `claimed_at` registra quando a linha foi
reservada para processamento. O claim atomico so considera uma linha PROCESSING
reclamavel de novo depois do lease (`CLAIM_LEASE_SECONDS`) expirar, o que impede
que duas entregas concorrentes do mesmo task_id executem o efeito colateral sem
quebrar as retentativas legitimas.

Sem indice novo: todo acesso a tabela e pela PRIMARY KEY (task_id).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# identificadores usados pelo Alembic
revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("tasks", "claimed_at")

"""add claim_id to tasks

Revision ID: 0003
Revises: 0002
Create Date: 2024-01-03 00:00:00.000000

Coluna de FENCING do claim idempotente: `claim_id` guarda a identidade da
reserva (um UUID novo a cada claim). As escritas de report-back (COMPLETED e
FAILED) casam por `task_id` E `claim_id`, portanto um worker zumbi -- que teve
o lease vencido e a linha reclamada por outro consumidor -- casa zero linhas e
nao sobrescreve o estado do consumidor vivo.

Sem indice novo: todo acesso a tabela e pela PRIMARY KEY (task_id).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# identificadores usados pelo Alembic
revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column("claim_id", postgresql.UUID(as_uuid=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("tasks", "claim_id")

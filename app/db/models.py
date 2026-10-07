"""Modelos ORM da aplicacao."""

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, Integer, String, Text, func, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


# str + Enum (e nao StrEnum) para manter o valor serializavel pelo Pydantic e
# o `.name` estavel para o tipo enum nativo do Postgres.
class TaskStatus(str, enum.Enum):  # noqa: UP042
    """Ciclo de vida de uma task."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Task(Base):
    """Resultado persistido do processamento de um evento.

    `task_id` e a PRIMARY KEY: e ela que garante a idempotencia, porque o claim
    usa INSERT ... ON CONFLICT (task_id) em uma unica instrucao atomica.

    `claimed_at` e o LEASE desse claim: enquanto estiver fresca a linha
    PROCESSING pertence a um consumidor; expirado o lease, a linha volta a ser
    reclamavel (worker morto ou tentativa que nunca reportou).
    """

    __tablename__ = "tasks"

    task_id: Mapped[uuid.UUID] = mapped_column(
        postgresql.UUID(as_uuid=True),
        primary_key=True,
    )
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(postgresql.JSONB, nullable=False)
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, name="task_status", native_enum=True, validate_strings=True),
        nullable=False,
        default=TaskStatus.PENDING,
        index=True,
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # NULL = nenhum claim pendente (linha nova, COMPLETED ou FAILED).
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(postgresql.JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    def __repr__(self) -> str:
        return f"<Task task_id={self.task_id} status={self.status} attempts={self.attempts}>"

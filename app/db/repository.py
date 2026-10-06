"""Acesso a dados das tasks.

O claim idempotente e uma unica instrucao atomica
(`INSERT ... ON CONFLICT (task_id) DO UPDATE ... WHERE status <> 'COMPLETED' RETURNING`),
portanto nao existe janela de corrida entre ler e escrever.

Os metodos que escrevem fazem commit: e o commit que libera o lock da linha e
torna o claim visivel para os outros consumidores.
"""

import uuid
from datetime import datetime
from typing import Any, Literal, Protocol, runtime_checkable

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Task, TaskStatus

ClaimResult = Literal["CLAIMED", "ALREADY_COMPLETED"]


@runtime_checkable
class TaskRepositoryProtocol(Protocol):
    """Contrato que o worker e a API consomem (os testes usam um fake)."""

    async def claim_for_processing(
        self,
        task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        attempts: int,
    ) -> ClaimResult: ...

    async def mark_completed(self, task_id: uuid.UUID, result: dict[str, Any]) -> None: ...

    async def mark_failed(self, task_id: uuid.UUID, error: str, attempts: int) -> None: ...

    async def get(self, task_id: uuid.UUID) -> Task | None: ...


class TaskRepository:
    """Implementacao do repositorio sobre uma AsyncSession do SQLAlchemy."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def claim_for_processing(
        self,
        task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        attempts: int,
    ) -> ClaimResult:
        """Reserva a task para processamento.

        Retorna "ALREADY_COMPLETED" quando a linha existente ja esta COMPLETED --
        nesse caso o `WHERE` do `DO UPDATE` nao casa, nada e retornado e a regra de
        negocio deve ser ignorada (garantia de idempotencia).
        """
        stmt = (
            pg_insert(Task)
            .values(
                task_id=task_id,
                event_type=event_type,
                payload=payload,
                status=TaskStatus.PROCESSING,
                attempts=attempts,
            )
            .on_conflict_do_update(
                index_elements=["task_id"],
                set_={
                    "status": TaskStatus.PROCESSING,
                    "attempts": attempts,
                    "updated_at": func.now(),
                },
                where=Task.status != TaskStatus.COMPLETED,
            )
            .returning(Task.task_id)
        )
        claimed = (await self.session.execute(stmt)).scalar_one_or_none()
        await self.session.commit()
        return "CLAIMED" if claimed is not None else "ALREADY_COMPLETED"

    async def mark_completed(self, task_id: uuid.UUID, result: dict[str, Any]) -> None:
        """Marca a task como COMPLETED e grava o resultado."""
        await self.session.execute(
            update(Task)
            .where(Task.task_id == task_id)
            .values(
                status=TaskStatus.COMPLETED,
                result=result,
                error=None,
                updated_at=func.now(),
            )
        )
        await self.session.commit()

    async def mark_failed(self, task_id: uuid.UUID, error: str, attempts: int) -> None:
        """Marca a task como FAILED, registrando o erro e o numero de tentativas."""
        await self.session.execute(
            update(Task)
            .where(Task.task_id == task_id)
            .values(
                status=TaskStatus.FAILED,
                error=error,
                attempts=attempts,
                updated_at=func.now(),
            )
        )
        await self.session.commit()

    async def get(self, task_id: uuid.UUID) -> Task | None:
        """Busca a task pela chave primaria."""
        return (
            await self.session.execute(select(Task).where(Task.task_id == task_id))
        ).scalar_one_or_none()

    async def create(
        self,
        task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        status: TaskStatus = TaskStatus.PENDING,
        attempts: int = 0,
        result: dict[str, Any] | None = None,
        error: str | None = None,
        created_at: datetime | None = None,
    ) -> Task | None:
        """Insere uma task; devolve None se a task_id ja existir (caminho de corrida)."""
        task = Task(
            task_id=task_id,
            event_type=event_type,
            payload=payload,
            status=status,
            attempts=attempts,
            result=result,
            error=error,
        )
        if created_at is not None:
            task.created_at = created_at

        self.session.add(task)
        try:
            await self.session.commit()
        except IntegrityError:
            await self.session.rollback()
            return None
        return task

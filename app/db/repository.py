"""Acesso a dados das tasks.

O claim idempotente e uma unica instrucao atomica
(`INSERT ... ON CONFLICT (task_id) DO UPDATE ... WHERE <reclamavel> RETURNING`),
portanto nao existe janela de corrida entre ler e escrever: a decisao de
processar E o resultado da propria instrucao (linha retornada ou nao).

O predicado de reclamavel usa um LEASE (`claimed_at` + `claim_lease_seconds`),
avaliado com o relogio do BANCO para nao sofrer skew entre workers:

| Estado da linha        | claimed_at    | Resultado          |
|------------------------|---------------|--------------------|
| inexistente            | --            | CLAIMED (INSERT)   |
| COMPLETED              | NULL          | ALREADY_COMPLETED  |
| FAILED / PENDING       | NULL          | CLAIMED (retry)    |
| PROCESSING, lease vivo | < lease       | LOCKED             |
| PROCESSING, lease velho| >= lease      | CLAIMED (orfa)     |

Todo claim grava um TOKEN novo (`claim_id`) e as duas escritas de report-back
(`mark_completed` e `mark_failed`) sao FENCED por ele: o `WHERE` exige
`claim_id = :claim_id AND status = 'PROCESSING'`, isto e, a escrita casa pela
identidade do claim que o escritor observou. Um predicado de lease
(`claimed_at > now() - lease`) NAO serviria: depois de o lease vencer e outro
consumidor reclamar a linha, o claim passa a estar fresco de novo e a escrita
do zumbi satisfaria o predicado. Com o token basta `TA <> TB`:

| Resultado   | Significado                                          |
|-------------|------------------------------------------------------|
| WRITTEN     | a linha era desta reserva e recebeu o estado final   |
| CLAIM_LOST  | outro consumidor detem o claim agora (escrita zumbi) |
| MISSING     | ninguem detem a task (linha ausente ou sem claim)    |

Os metodos que escrevem fazem commit: e o commit que libera o lock da linha e
torna o claim visivel para os outros consumidores.
"""

import uuid
from datetime import datetime
from typing import Any, Literal, Protocol, runtime_checkable

from sqlalchemy import and_, func, literal, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.db.models import Task, TaskStatus

ClaimResult = Literal["CLAIMED", "ALREADY_COMPLETED", "LOCKED"]
FencedWriteResult = Literal["WRITTEN", "CLAIM_LOST", "MISSING"]


@runtime_checkable
class TaskRepositoryProtocol(Protocol):
    """Contrato que o worker e a API consomem (os testes usam um fake)."""

    async def claim_for_processing(
        self,
        task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        attempts: int,
        *,
        claim_id: uuid.UUID,
    ) -> ClaimResult: ...

    async def mark_completed(
        self,
        task_id: uuid.UUID,
        result: dict[str, Any],
        *,
        claim_id: uuid.UUID,
    ) -> FencedWriteResult: ...

    async def mark_failed(
        self,
        task_id: uuid.UUID,
        error: str,
        attempts: int,
        *,
        claim_id: uuid.UUID,
    ) -> FencedWriteResult: ...

    async def get(self, task_id: uuid.UUID) -> Task | None: ...


class TaskRepository:
    """Implementacao do repositorio sobre uma AsyncSession do SQLAlchemy."""

    def __init__(self, session: AsyncSession, settings: Settings | None = None) -> None:
        self.session = session
        self._claim_lease_seconds = (settings or get_settings()).claim_lease_seconds

    async def claim_for_processing(
        self,
        task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        attempts: int,
        *,
        claim_id: uuid.UUID,
    ) -> ClaimResult:
        """Reserva a task para processamento em UMA instrucao atomica.

        "CLAIMED" significa que esta entrega ganhou o claim e deve executar a
        regra de negocio. Qualquer outro resultado significa que ela NAO deve:
        "ALREADY_COMPLETED" quando a linha ja terminou (duplicata espacada) e
        "LOCKED" quando outro consumidor detem um claim ainda dentro do lease
        (duplicata concorrente).

        `claim_id` e o token desta reserva: gravado aqui, exigido depois pelas
        duas escritas de report-back.
        """
        # Corte do lease pelo relogio do banco; o valor vai como bind parameter.
        lease_cutoff = func.now() - literal(self._claim_lease_seconds) * text("interval '1 second'")
        stmt = (
            pg_insert(Task)
            .values(
                task_id=task_id,
                event_type=event_type,
                payload=payload,
                status=TaskStatus.PROCESSING,
                attempts=attempts,
                claimed_at=func.now(),
                claim_id=claim_id,
            )
            .on_conflict_do_update(
                index_elements=["task_id"],
                set_={
                    "status": TaskStatus.PROCESSING,
                    "attempts": attempts,
                    "claimed_at": func.now(),
                    "claim_id": claim_id,
                    "updated_at": func.now(),
                },
                where=and_(
                    Task.status != TaskStatus.COMPLETED,
                    or_(
                        Task.status != TaskStatus.PROCESSING,
                        Task.claimed_at.is_(None),
                        Task.claimed_at < lease_cutoff,
                    ),
                ),
            )
            .returning(Task.task_id)
        )
        claimed = (await self.session.execute(stmt)).scalar_one_or_none()
        await self.session.commit()
        if claimed is not None:
            return "CLAIMED"

        # A decisao de processar JA foi tomada pela instrucao acima (nada
        # retornou => nao processa). Esta leitura so rotula o motivo para o log.
        status = (
            await self.session.execute(select(Task.status).where(Task.task_id == task_id))
        ).scalar_one_or_none()
        return "ALREADY_COMPLETED" if status == TaskStatus.COMPLETED else "LOCKED"

    async def _report_back(
        self,
        task_id: uuid.UUID,
        claim_id: uuid.UUID,
        values: dict[str, Any],
    ) -> FencedWriteResult:
        """Escrita de estado final FENCED pelo token do claim.

        Uma unica instrucao, sem leitura previa: o `RETURNING` vazio E a
        rejeicao. Zerar `claim_id` junto com `claimed_at` deixa a report-back
        at-most-once (a mesma chamada repetida nao casa de novo) e faz uma
        linha COMPLETED/FAILED nao casar com token nenhum.
        """
        written = (
            await self.session.execute(
                update(Task)
                .where(
                    Task.task_id == task_id,
                    Task.claim_id == claim_id,
                    Task.status == TaskStatus.PROCESSING,
                )
                .values(**values, claimed_at=None, claim_id=None, updated_at=func.now())
                .returning(Task.task_id)
            )
        ).scalar_one_or_none()
        await self.session.commit()
        if written is not None:
            return "WRITTEN"

        # A decisao JA foi tomada pela instrucao acima (nada retornou => nao
        # escreveu). Esta leitura so rotula o motivo, e o rotulo importa: so
        # vale dar ack sem retentar quando a linha JA PERTENCE a outra entrega
        # (ou ja foi finalizada por ela), porque essa outra entrega resolve a
        # task. Linha ausente significa o oposto -- ninguem detem a task -- e
        # ali a cadeia de retry/DLX precisa continuar.
        row = (
            await self.session.execute(select(Task.task_id).where(Task.task_id == task_id))
        ).one_or_none()
        return "MISSING" if row is None else "CLAIM_LOST"

    async def mark_completed(
        self,
        task_id: uuid.UUID,
        result: dict[str, Any],
        *,
        claim_id: uuid.UUID,
    ) -> FencedWriteResult:
        """Marca a task como COMPLETED, grava o resultado e libera o claim."""
        return await self._report_back(
            task_id,
            claim_id,
            {"status": TaskStatus.COMPLETED, "result": result, "error": None},
        )

    async def mark_failed(
        self,
        task_id: uuid.UUID,
        error: str,
        attempts: int,
        *,
        claim_id: uuid.UUID,
    ) -> FencedWriteResult:
        """Marca a task como FAILED, registra o erro e libera o claim.

        Zerar `claimed_at` aqui e o que mantem a retentativa funcionando: a
        entrega seguinte encontra uma linha FAILED sem claim pendente e a
        reclama na hora, sem esperar o lease expirar.
        """
        return await self._report_back(
            task_id,
            claim_id,
            {"status": TaskStatus.FAILED, "error": error, "attempts": attempts},
        )

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

"""Dubles em memoria usados pelos testes unitarios (nenhum servico externo)."""

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from aio_pika import DeliveryMode

from app.core.broker import BODY_EVENT_TYPE, BODY_PAYLOAD, BODY_TASK_ID, CONTENT_TYPE_JSON
from app.core.config import Settings
from app.core.constants import (
    X_FAILED_AT_HEADER,
    X_ORIGINAL_EXCHANGE_HEADER,
    X_ORIGINAL_ROUTING_KEY_HEADER,
)
from app.core.exceptions import TaskProcessingError
from app.db.models import Task, TaskStatus
from app.db.repository import ClaimResult


def _now() -> datetime:
    return datetime.now(UTC)


def make_task(
    task_id: uuid.UUID,
    event_type: str = "demo",
    payload: dict[str, Any] | None = None,
    status: TaskStatus = TaskStatus.PENDING,
    attempts: int = 0,
    result: dict[str, Any] | None = None,
    error: str | None = None,
    claimed_at: datetime | None = None,
) -> Task:
    """Instancia de Task completa (com timestamps) para uso fora do banco."""
    now = _now()
    return Task(
        task_id=task_id,
        event_type=event_type,
        payload=payload if payload is not None else {},
        status=status,
        attempts=attempts,
        result=result,
        error=error,
        claimed_at=claimed_at,
        created_at=now,
        updated_at=now,
    )


class FakeTaskRepository:
    """TaskRepositoryProtocol em memoria com a mesma semantica de claim.

    Espelha a tabela de verdade de `app/db/repository.py`, lease incluido: um
    claim PROCESSING ainda dentro de `claim_lease_seconds` devolve "LOCKED".
    """

    def __init__(self, claim_lease_seconds: float = 5.0) -> None:
        self.rows: dict[uuid.UUID, Task] = {}
        self.claim_lease_seconds = claim_lease_seconds

    def seed(self, task: Task) -> Task:
        """Insere uma linha pronta (atalho de arrange dos testes)."""
        self.rows[task.task_id] = task
        return task

    def expire_claim(self, task_id: uuid.UUID) -> None:
        """Backdata o `claimed_at` para fora do lease (claim obsoleto).

        Deterministico e instantaneo: nenhum teste precisa dormir o lease.
        """
        self.rows[task_id].claimed_at = _now() - timedelta(seconds=self.claim_lease_seconds + 1)

    async def claim_for_processing(
        self,
        task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
        attempts: int,
    ) -> ClaimResult:
        """Mesma regra do repositorio real (COMPLETED e lease vivo nao sao reclamados)."""
        existing = self.rows.get(task_id)
        if existing is not None:
            if existing.status == TaskStatus.COMPLETED:
                return "ALREADY_COMPLETED"
            if existing.status == TaskStatus.PROCESSING and self._lease_is_alive(existing):
                return "LOCKED"

        if existing is None:
            self.rows[task_id] = make_task(
                task_id=task_id,
                event_type=event_type,
                payload=payload,
                status=TaskStatus.PROCESSING,
                attempts=attempts,
                claimed_at=_now(),
            )
        else:
            existing.status = TaskStatus.PROCESSING
            existing.attempts = attempts
            existing.claimed_at = _now()
            existing.updated_at = _now()
        return "CLAIMED"

    def _lease_is_alive(self, task: Task) -> bool:
        if task.claimed_at is None:
            return False
        return _now() - task.claimed_at < timedelta(seconds=self.claim_lease_seconds)

    async def mark_completed(self, task_id: uuid.UUID, result: dict[str, Any]) -> None:
        task = self.rows[task_id]
        task.status = TaskStatus.COMPLETED
        task.result = result
        task.error = None
        task.claimed_at = None
        task.updated_at = _now()

    async def mark_failed(self, task_id: uuid.UUID, error: str, attempts: int) -> None:
        task = self.rows[task_id]
        task.status = TaskStatus.FAILED
        task.error = error
        task.attempts = attempts
        task.claimed_at = None
        task.updated_at = _now()

    async def get(self, task_id: uuid.UUID) -> Task | None:
        return self.rows.get(task_id)


class FakeIncomingMessage:
    """Substitui AbstractIncomingMessage registrando os ack/nack recebidos."""

    def __init__(
        self,
        body: bytes,
        headers: dict[str, Any] | None = None,
        exchange: str = "",
        routing_key: str = "",
    ) -> None:
        self.body = body
        self.headers: dict[str, Any] = headers if headers is not None else {}
        self.exchange = exchange
        self.routing_key = routing_key
        self.acks = 0
        self.nacks: list[bool] = []

    @property
    def settled(self) -> int:
        """Quantas vezes a mensagem foi resolvida (ack + nack)."""
        return self.acks + len(self.nacks)

    async def ack(self) -> None:
        self.acks += 1

    async def nack(self, requeue: bool = True) -> None:
        self.nacks.append(requeue)


@dataclass(frozen=True, slots=True)
class PublishedMessage:
    """Registro de uma publicacao feita pelo FakePublisher."""

    exchange: str
    routing_key: str
    delivery_mode: DeliveryMode
    body: bytes
    content_type: str = CONTENT_TYPE_JSON
    message_id: str | None = None
    headers: dict[str, Any] = field(default_factory=dict)

    @property
    def decoded(self) -> dict[str, Any]:
        """Corpo JSON desserializado."""
        return json.loads(self.body)


class FakePublisher:
    """Substitui TaskPublisher acumulando o que seria publicado."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.messages: list[PublishedMessage] = []

    @property
    def task_messages(self) -> list[PublishedMessage]:
        """Publicacoes na exchange de trabalho."""
        return [m for m in self.messages if m.exchange == self._settings.tasks_exchange_name]

    @property
    def dlx_messages(self) -> list[PublishedMessage]:
        """Publicacoes na dead letter exchange."""
        return [m for m in self.messages if m.exchange == self._settings.dlx_exchange_name]

    async def publish(
        self,
        task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        body = json.dumps(
            {
                BODY_TASK_ID: str(task_id),
                BODY_EVENT_TYPE: event_type,
                BODY_PAYLOAD: payload,
            }
        ).encode()
        self.messages.append(
            PublishedMessage(
                exchange=self._settings.tasks_exchange_name,
                routing_key=self._settings.tasks_routing_key,
                delivery_mode=DeliveryMode.PERSISTENT,
                body=body,
                message_id=str(task_id),
            )
        )

    async def publish_to_dlx(
        self,
        body: bytes,
        headers: dict[str, Any],
        original_exchange: str,
        original_routing_key: str,
    ) -> None:
        dlx_headers: dict[str, Any] = {
            **headers,
            X_ORIGINAL_EXCHANGE_HEADER: original_exchange,
            X_ORIGINAL_ROUTING_KEY_HEADER: original_routing_key,
        }
        dlx_headers.setdefault(X_FAILED_AT_HEADER, _now().isoformat())
        self.messages.append(
            PublishedMessage(
                exchange=self._settings.dlx_exchange_name,
                routing_key=self._settings.dlx_routing_key,
                delivery_mode=DeliveryMode.PERSISTENT,
                body=body,
                headers=dlx_headers,
            )
        )


class CountingProcessor:
    """Spy de processamento: conta execucoes e pode falhar as N primeiras."""

    def __init__(
        self,
        result: dict[str, Any] | None = None,
        fail_times: int = 0,
        always_fail: bool = False,
    ) -> None:
        self.result = result if result is not None else {"processed": True}
        self.fail_times = fail_times
        self.always_fail = always_fail
        self.calls: list[tuple[uuid.UUID, str, dict[str, Any]]] = []

    @property
    def count(self) -> int:
        """Quantidade de execucoes observadas."""
        return len(self.calls)

    async def __call__(
        self,
        task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        self.calls.append((task_id, event_type, payload))
        if self.always_fail or self.count <= self.fail_times:
            raise TaskProcessingError(f"forced failure on attempt {self.count}")
        return self.result

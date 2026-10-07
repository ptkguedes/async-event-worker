"""Regra de negocio do worker: processamento simulado + garantia de idempotencia.

Separado do entrypoint (app/worker/main.py) de proposito: aqui nao existe AMQP,
so o corpo da mensagem, o repositorio e o callable de processamento -- o que
permite testar a idempotencia com um fake em memoria e um spy contador.
"""

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from app.core.broker import BODY_EVENT_TYPE, BODY_PAYLOAD, BODY_TASK_ID
from app.core.config import Settings, get_settings
from app.core.constants import FORCE_FAILURE_KEY
from app.core.exceptions import TaskProcessingError
from app.db.repository import TaskRepositoryProtocol

logger = logging.getLogger(__name__)

HandleOutcome = Literal["PROCESSED", "SKIPPED_DUPLICATE", "SKIPPED_CONCURRENT"]

# Assinatura do callable injetavel (os testes passam um spy contador).
Processor = Callable[[uuid.UUID, str, dict[str, Any]], Awaitable[dict[str, Any]]]

RESULT_PROCESSED_AT = "processed_at"
RESULT_EVENT_TYPE = "event_type"
RESULT_ECHO = "echo"


@dataclass(frozen=True, slots=True)
class TaskMessage:
    """Corpo da mensagem de task ja validado."""

    task_id: uuid.UUID
    event_type: str
    payload: dict[str, Any]


def parse_task_message(raw: Any) -> TaskMessage:
    """Valida o corpo decodificado da mensagem.

    Levanta `ValueError` quando o corpo nao e um objeto JSON com `task_id` (UUID)
    e `event_type` -- payload invalido nao vale retentar, o chamador manda direto
    para a dead letter queue.
    """
    if not isinstance(raw, dict):
        raise ValueError("message body must be a JSON object")

    try:
        task_id = uuid.UUID(str(raw[BODY_TASK_ID]))
    except (KeyError, ValueError, TypeError) as exc:
        raise ValueError(f"message body has an invalid {BODY_TASK_ID}: {exc}") from exc

    event_type = raw.get(BODY_EVENT_TYPE)
    if not isinstance(event_type, str) or not event_type:
        raise ValueError(f"message body has an invalid {BODY_EVENT_TYPE}")

    payload = raw.get(BODY_PAYLOAD) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"message body has an invalid {BODY_PAYLOAD}")

    return TaskMessage(task_id=task_id, event_type=event_type, payload=payload)


async def simulate_processing(
    task_id: uuid.UUID,
    event_type: str,
    payload: dict[str, Any],
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Simula o trabalho pesado da task.

    Dorme `settings.processing_delay_seconds` e levanta `TaskProcessingError`
    quando o payload traz a chave `force_failure` verdadeira -- esse caminho de
    falha deliberado e o que torna o fluxo de retry/DLX demonstravel e testavel
    sem mexer no codigo do worker.
    """
    app_settings = settings or get_settings()
    await asyncio.sleep(app_settings.processing_delay_seconds)

    if payload.get(FORCE_FAILURE_KEY):
        raise TaskProcessingError(
            f"processing failed on purpose ({FORCE_FAILURE_KEY}=true) for task {task_id}"
        )

    return {
        RESULT_PROCESSED_AT: datetime.now(UTC).isoformat(),
        RESULT_EVENT_TYPE: event_type,
        RESULT_ECHO: payload,
    }


async def handle_task(
    message_data: TaskMessage,
    repo: TaskRepositoryProtocol,
    attempts: int,
    processor: Processor = simulate_processing,
) -> HandleOutcome:
    """Processa a task uma unica vez, mesmo com entregas duplicadas.

    A GARANTIA DE IDEMPOTENCIA vem do claim atomico feito ANTES de qualquer regra
    de negocio. So o resultado "CLAIMED" autoriza chamar o `processor`:

    - "ALREADY_COMPLETED": a linha ja terminou (duplicata espacada) -- o
      chamador da ack e nada e reexecutado.
    - "LOCKED": outro consumidor detem o claim AGORA (duplicata concorrente, as
      duas entregas na mesma janela de prefetch) -- o chamador devolve a
      mensagem para o hop de retry.

    `TaskProcessingError` levantada pelo processor sobe para o chamador (o callback
    AMQP), que decide entre retentar e mandar para a dead letter queue.
    """
    claim = await repo.claim_for_processing(
        message_data.task_id,
        message_data.event_type,
        message_data.payload,
        attempts,
    )
    log_context = {
        "task_id": str(message_data.task_id),
        "event_type": message_data.event_type,
        "attempts": attempts,
    }

    if claim == "ALREADY_COMPLETED":
        logger.info("duplicate delivery skipped", extra=log_context)
        return "SKIPPED_DUPLICATE"

    if claim == "LOCKED":
        logger.warning("concurrent claim skipped", extra=log_context)
        return "SKIPPED_CONCURRENT"

    result = await processor(
        message_data.task_id,
        message_data.event_type,
        message_data.payload,
    )
    await repo.mark_completed(message_data.task_id, result)
    logger.info("task processed", extra=log_context)
    return "PROCESSED"

"""FLUXO B -- idempotencia: duas entregas do mesmo task_id, um unico efeito.

A duplicata e confirmada (ack) sem reexecutar a regra de negocio e sem criar uma
segunda linha, porque `handle_task` faz o claim atomico por `task_id` (PRIMARY
KEY) ANTES de chamar o processor.
"""

import uuid

from aio_pika.abc import AbstractChannel

from app.core.broker import TaskPublisher
from app.core.config import Settings
from app.core.topology import DeclaredTopology
from app.db.models import TaskStatus
from app.worker.handlers import RESULT_ECHO
from tests.integration.conftest import (
    count_tasks,
    fetch_task,
    queue_message_count,
    wait_for_empty_queue,
    wait_for_task,
)

EVENT_TYPE = "integration.idempotency"


async def test_duplicate_delivery_is_processed_once(
    settings: Settings,
    purged_topology: DeclaredTopology,
    broker_channel: AbstractChannel,
    running_worker: None,
) -> None:
    """Duas mensagens com o MESMO task_id produzem exatamente uma linha."""
    task_id = uuid.uuid4()
    publisher = TaskPublisher.from_topology(purged_topology, settings)

    # Payloads diferentes de proposito: o resultado persistido prova qual das
    # duas entregas realmente executou o processamento.
    await publisher.publish(task_id, EVENT_TYPE, {"delivery": "first"})
    await publisher.publish(task_id, EVENT_TYPE, {"delivery": "second"})

    await wait_for_task(settings, task_id, lambda row: row.status == TaskStatus.COMPLETED)
    # A duplicata tambem recebe ack: a fila de trabalho precisa drenar.
    await wait_for_empty_queue(broker_channel, settings.tasks_queue_name)

    # Releitura DEPOIS do ack da duplicata: se ela tivesse reexecutado, o
    # resultado persistido teria sido sobrescrito pela segunda entrega.
    task = await fetch_task(settings, task_id)
    assert task is not None
    assert await count_tasks(settings, task_id) == 1
    assert task.status == TaskStatus.COMPLETED
    assert task.attempts == 1
    assert task.result is not None
    assert task.result[RESULT_ECHO] == {"delivery": "first"}
    assert await queue_message_count(broker_channel, settings.dlx_queue_name) == 0

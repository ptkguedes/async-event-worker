"""FLUXO A -- caminho felix: publicar, consumir e persistir o resultado.

Exercita a cadeia completa com servicos reais: `TaskPublisher` -> exchange de
trabalho -> fila `test_tasks` -> worker in-process -> PostgreSQL.
"""

import uuid

from aio_pika.abc import AbstractChannel

from app.core.broker import TaskPublisher
from app.core.config import Settings
from app.core.topology import DeclaredTopology
from app.db.models import TaskStatus
from app.worker.handlers import RESULT_ECHO, RESULT_EVENT_TYPE
from tests.integration.conftest import (
    queue_message_count,
    wait_for_empty_queue,
    wait_for_task,
)

EVENT_TYPE = "integration.success"


async def test_published_task_is_processed_and_persisted(
    settings: Settings,
    purged_topology: DeclaredTopology,
    broker_channel: AbstractChannel,
    running_worker: None,
) -> None:
    """A task publicada termina COMPLETED, com resultado e uma unica tentativa."""
    task_id = uuid.uuid4()
    payload = {"order_id": 42, "items": ["a", "b"]}
    publisher = TaskPublisher.from_topology(purged_topology, settings)

    await publisher.publish(task_id, EVENT_TYPE, payload)

    task = await wait_for_task(
        settings,
        task_id,
        lambda row: row.status == TaskStatus.COMPLETED,
    )

    assert task.event_type == EVENT_TYPE
    assert task.payload == payload
    assert task.result is not None
    assert task.result[RESULT_EVENT_TYPE] == EVENT_TYPE
    assert task.result[RESULT_ECHO] == payload
    assert task.error is None
    assert task.attempts == 1

    # Nada sobrou na fila de trabalho e nada foi para a dead letter queue.
    await wait_for_empty_queue(broker_channel, settings.tasks_queue_name)
    assert await queue_message_count(broker_channel, settings.dlx_queue_name) == 0

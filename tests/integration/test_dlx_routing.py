"""FLUXO C -- Dead Letter Exchange depois das retentativas esgotadas.

Com `task_max_retries=3`, uma task que falha sempre e entregue 4 vezes na fila
de trabalho (1 original + 3 retentativas). O atraso entre as tentativas vem do
`x-message-ttl` da fila de retry, nao de codigo de aplicacao. Na 4a falha o
worker publica o corpo ORIGINAL em `test_dlx_tasks` com os headers de
diagnostico e da ack na mensagem original.
"""

import json
import uuid

from aio_pika.abc import AbstractChannel

from app.core.broker import BODY_TASK_ID, TaskPublisher
from app.core.config import Settings
from app.core.constants import (
    FORCE_FAILURE_KEY,
    X_ATTEMPTS_HEADER,
    X_FAILURE_REASON_HEADER,
    X_ORIGINAL_EXCHANGE_HEADER,
    X_ORIGINAL_ROUTING_KEY_HEADER,
    X_RETRY_COUNT_HEADER,
)
from app.core.topology import DeclaredTopology
from app.db.models import TaskStatus
from tests.integration.conftest import (
    queue_message_count,
    wait_for_message,
    wait_for_task,
)

EVENT_TYPE = "integration.dlx"

# 1 original + 3 retentativas com TTL de 500ms, mais folga para o broker.
DLX_TIMEOUT_SECONDS = 15.0
EXPECTED_RETRY_COUNT = 3
EXPECTED_ATTEMPTS = 4


async def test_failing_task_ends_up_in_dead_letter_queue(
    settings: Settings,
    purged_topology: DeclaredTopology,
    broker_channel: AbstractChannel,
    running_worker: None,
) -> None:
    """A task que sempre falha termina na dead letter queue e FAILED no banco."""
    task_id = uuid.uuid4()
    publisher = TaskPublisher.from_topology(purged_topology, settings)

    await publisher.publish(task_id, EVENT_TYPE, {FORCE_FAILURE_KEY: True})

    dead_message = await wait_for_message(purged_topology.dlx_queue, timeout=DLX_TIMEOUT_SECONDS)

    # Corpo ORIGINAL preservado, headers de diagnostico presentes.
    assert json.loads(dead_message.body)[BODY_TASK_ID] == str(task_id)
    headers = dict(dead_message.headers)
    assert headers[X_RETRY_COUNT_HEADER] == EXPECTED_RETRY_COUNT
    assert headers[X_ATTEMPTS_HEADER] == EXPECTED_ATTEMPTS
    assert str(headers[X_FAILURE_REASON_HEADER])
    assert str(headers[X_ORIGINAL_EXCHANGE_HEADER]) == settings.tasks_exchange_name
    assert str(headers[X_ORIGINAL_ROUTING_KEY_HEADER]) == settings.tasks_routing_key

    # Exatamente 1 mensagem morta: a leitura acima consumiu a unica que existia.
    assert await purged_topology.dlx_queue.get(no_ack=True, fail=False) is None

    task = await wait_for_task(
        settings,
        task_id,
        lambda row: row.attempts == EXPECTED_ATTEMPTS,
        timeout=DLX_TIMEOUT_SECONDS,
    )
    assert task.status == TaskStatus.FAILED
    assert task.error is not None
    assert task.result is None

    # A mensagem nao ficou presa nem na fila de trabalho nem na de retry.
    assert await queue_message_count(broker_channel, settings.tasks_queue_name) == 0
    assert await queue_message_count(broker_channel, settings.retry_queue_name) == 0

"""FLUXO B -- idempotencia: duas entregas do mesmo task_id, um unico efeito.

A duplicata e confirmada (ack) sem reexecutar a regra de negocio e sem criar uma
segunda linha, porque `handle_task` faz o claim atomico por `task_id` (PRIMARY
KEY) ANTES de chamar o processor.

Dois niveis de cobertura aqui:

1. duplicata ESPACADA (pelo broker, com o worker de verdade): a segunda entrega
   chega com a linha ja COMPLETED;
2. duplicata CONCORRENTE (duas `handle_task` sobrepostas, sem broker): as duas
   entregas disputam o MESMO claim na mesma janela -- e o caso que o lease
   (`claimed_at` + `CLAIM_LEASE_SECONDS`) resolve, contando o efeito colateral
   direto no spy em vez de so olhar "tem 1 linha".
"""

import asyncio
import uuid

from aio_pika.abc import AbstractChannel
from sqlalchemy import text

from app.core.broker import TaskPublisher
from app.core.config import Settings
from app.core.topology import DeclaredTopology
from app.db.models import TaskStatus
from app.db.repository import TaskRepository
from app.db.session import session_scope
from app.worker.handlers import RESULT_ECHO, HandleOutcome, TaskMessage, handle_task
from tests.fakes import CountingProcessor
from tests.integration.conftest import (
    count_tasks,
    fetch_task,
    queue_message_count,
    wait_for_empty_queue,
    wait_for_task,
)

EVENT_TYPE = "integration.idempotency"
CONCURRENT_RESULT = {"concurrent": True}
SKIPPED_OUTCOMES = {"SKIPPED_CONCURRENT", "SKIPPED_DUPLICATE"}


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


async def _deliver(
    settings: Settings,
    message: TaskMessage,
    processor: CountingProcessor,
) -> HandleOutcome:
    """Uma entrega completa, com sessao e repositorio proprios (como no worker)."""
    async with session_scope(settings) as session:
        return await handle_task(
            message,
            TaskRepository(session, settings),
            attempts=1,
            processor=processor,
        )


async def test_concurrent_duplicate_runs_the_side_effect_exactly_once(
    settings: Settings,
    services: None,
) -> None:
    """Duas entregas SOBREPOSTAS do mesmo task_id: o processor roda UMA vez.

    Este e o defeito corrigido: antes do lease, duas entregas dentro da mesma
    janela de prefetch ganhavam o claim as duas (ambas viam a linha ainda nao
    COMPLETED) e executavam o efeito colateral duas vezes. A contagem vem do
    spy compartilhado, nao da quantidade de linhas.
    """
    task_id = uuid.uuid4()
    message = TaskMessage(task_id=task_id, event_type=EVENT_TYPE, payload={"delivery": "race"})
    processor = CountingProcessor(result=CONCURRENT_RESULT)

    outcomes = await asyncio.gather(
        _deliver(settings, message, processor),
        _deliver(settings, message, processor),
    )

    assert processor.count == 1, f"o efeito colateral rodou {processor.count} vezes"
    assert outcomes.count("PROCESSED") == 1
    assert {o for o in outcomes if o != "PROCESSED"} <= SKIPPED_OUTCOMES

    assert await count_tasks(settings, task_id) == 1
    task = await fetch_task(settings, task_id)
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert task.attempts == 1
    assert task.result == CONCURRENT_RESULT
    assert task.claimed_at is None


async def test_claim_lease_locks_a_live_claim_and_frees_a_stale_one(
    settings: Settings,
    services: None,
) -> None:
    """O lease distingue claim vivo (LOCKED) de claim orfao (reclamavel)."""
    task_id = uuid.uuid4()

    async with session_scope(settings) as session:
        repo = TaskRepository(session, settings)
        assert await repo.claim_for_processing(task_id, EVENT_TYPE, {}, 1) == "CLAIMED"
        # Claim ainda dentro do lease: a segunda entrega nao pode processar.
        assert await repo.claim_for_processing(task_id, EVENT_TYPE, {}, 1) == "LOCKED"

        # Backdating em vez de sleep: o claim passa a estar fora do lease.
        await session.execute(
            text(
                "UPDATE tasks SET claimed_at = now() - make_interval(secs => :age)"
                " WHERE task_id = :task_id"
            ),
            {"age": settings.claim_lease_seconds + 1, "task_id": task_id},
        )
        await session.commit()

        assert await repo.claim_for_processing(task_id, EVENT_TYPE, {}, 2) == "CLAIMED"

        # COMPLETED nunca volta a ser reclamavel, lease ou nao.
        await repo.mark_completed(task_id, {"done": True})
        assert await repo.claim_for_processing(task_id, EVENT_TYPE, {}, 3) == "ALREADY_COMPLETED"

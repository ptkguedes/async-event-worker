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
   direto no spy em vez de so olhar "tem 1 linha". A corrida e forcada por
   `asyncio.Event`, nao por tempo: o worker 1 fica PRESO dentro da secao
   critica ate o worker 2 ter tentado o claim, portanto o worker 2
   obrigatoriamente cai no ramo de concorrencia.
3. FENCING das report-backs (`claim_id`): um worker zumbi, cujo lease venceu e
   cuja linha foi reclamada por outro consumidor, nao consegue sobrescrever o
   estado do consumidor vivo.
"""

import asyncio
import uuid
from typing import Any

from aio_pika.abc import AbstractChannel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.broker import TaskPublisher
from app.core.config import Settings
from app.core.topology import DeclaredTopology
from app.db.models import TaskStatus
from app.db.repository import TaskRepository
from app.db.session import session_scope
from app.worker.handlers import (
    RESULT_ECHO,
    HandleOutcome,
    Processor,
    TaskMessage,
    handle_task,
)
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
ZOMBIE_RESULT = {"written_by": "zombie"}
OWNER_RESULT = {"written_by": "owner"}

# Teto de qualquer espera de sincronizacao: um erro de logica falha rapido e
# explicado em vez de pendurar a suite. Nenhuma espera aqui e por tempo.
GATE_TIMEOUT_SECONDS = 5.0


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
    processor: Processor,
) -> HandleOutcome:
    """Uma entrega completa, com sessao, repositorio e token proprios."""
    async with session_scope(settings) as session:
        return await handle_task(
            message,
            TaskRepository(session, settings),
            attempts=1,
            processor=processor,
            claim_id=uuid.uuid4(),
        )


async def _wait_for_gate(event: asyncio.Event, what: str) -> None:
    """Espera um evento de sincronizacao com teto e mensagem explicita."""
    try:
        await asyncio.wait_for(event.wait(), timeout=GATE_TIMEOUT_SECONDS)
    except TimeoutError as exc:
        raise AssertionError(f"timeout de {GATE_TIMEOUT_SECONDS}s esperando {what}") from exc


async def _expire_claim(session: AsyncSession, settings: Settings, task_id: uuid.UUID) -> None:
    """Backdata o `claimed_at` para fora do lease (sem sleep, deterministico)."""
    await session.execute(
        text(
            "UPDATE tasks SET claimed_at = now() - make_interval(secs => :age)"
            " WHERE task_id = :task_id"
        ),
        {"age": settings.claim_lease_seconds + 1, "task_id": task_id},
    )
    await session.commit()


async def test_concurrent_duplicate_runs_the_side_effect_exactly_once(
    settings: Settings,
    services: None,
) -> None:
    """Duas entregas SOBREPOSTAS do mesmo task_id: o processor roda UMA vez.

    Este e o defeito corrigido: antes do lease, duas entregas dentro da mesma
    janela de prefetch ganhavam o claim as duas (ambas viam a linha ainda nao
    COMPLETED) e executavam o efeito colateral duas vezes. A contagem vem do
    spy compartilhado, nao da quantidade de linhas.

    A corrida e FORCADA, nao esperada: o processor do worker 1 sinaliza
    `claim_taken` (o claim ja esta commitado, a report-back ainda nao) e so sai
    quando `release` for setado, o que o worker 2 faz DEPOIS de a tentativa de
    claim dele ter retornado. Logo o worker 2 sempre encontra um claim vivo e o
    unico outcome possivel para ele e "SKIPPED_CONCURRENT".
    """
    task_id = uuid.uuid4()
    message = TaskMessage(task_id=task_id, event_type=EVENT_TYPE, payload={"delivery": "race"})
    processor = CountingProcessor(result=CONCURRENT_RESULT)
    claim_taken = asyncio.Event()
    release = asyncio.Event()

    async def gated_processor(
        inner_task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Trava o worker 1 DENTRO da secao critica e delega ao spy."""
        claim_taken.set()
        await _wait_for_gate(release, "o worker 2 tentar o claim")
        return await processor(inner_task_id, event_type, payload)

    async def worker_one() -> HandleOutcome:
        return await _deliver(settings, message, gated_processor)

    async def worker_two() -> HandleOutcome:
        await _wait_for_gate(claim_taken, "o worker 1 tomar o claim")
        try:
            return await _deliver(settings, message, processor)
        finally:
            # No finally: nem uma excecao aqui pode deixar o worker 1 preso.
            release.set()

    outcome_one, outcome_two = await asyncio.gather(worker_one(), worker_two())

    assert processor.count == 1, f"o efeito colateral rodou {processor.count} vezes"
    assert outcome_one == "PROCESSED"
    assert outcome_two == "SKIPPED_CONCURRENT"

    assert await count_tasks(settings, task_id) == 1
    task = await fetch_task(settings, task_id)
    assert task is not None
    assert task.status == TaskStatus.COMPLETED
    assert task.attempts == 1
    assert task.result == CONCURRENT_RESULT
    assert task.claimed_at is None
    assert task.claim_id is None


async def test_a_zombie_report_back_cannot_overwrite_the_live_owner(
    settings: Settings,
    services: None,
) -> None:
    """Fencing no Postgres de verdade: as escritas do zumbi casam ZERO linhas.

    Roteiro: A reclama (token TA), o lease vence, B reclama a mesma linha
    (token TB) e so entao A acorda. As duas report-backs de A sao rejeitadas, a
    linha segue sendo de B, e nem depois de B concluir o `mark_failed` atrasado
    de A consegue reverter o COMPLETED para um FAILED reclamavel.
    """
    task_id = uuid.uuid4()
    token_a = uuid.uuid4()
    token_b = uuid.uuid4()

    async with session_scope(settings) as session:
        repo = TaskRepository(session, settings)
        assert (
            await repo.claim_for_processing(task_id, EVENT_TYPE, {}, 1, claim_id=token_a)
            == "CLAIMED"
        )

        # A travou: backdating em vez de sleep, e B reclama a linha orfa.
        await _expire_claim(session, settings, task_id)
        assert (
            await repo.claim_for_processing(task_id, EVENT_TYPE, {}, 2, claim_id=token_b)
            == "CLAIMED"
        )

        # A acorda. Nenhuma das duas escritas dele pode casar a linha.
        assert await repo.mark_completed(task_id, ZOMBIE_RESULT, claim_id=token_a) == "CLAIM_LOST"
        assert await repo.mark_failed(task_id, "A travou", 1, claim_id=token_a) == "CLAIM_LOST"

        held = await fetch_task(settings, task_id)
        assert held is not None
        assert held.status == TaskStatus.PROCESSING
        assert held.claim_id == token_b
        assert held.attempts == 2
        assert held.result is None
        assert held.error is None

        # O dono vivo escreve normalmente...
        assert await repo.mark_completed(task_id, OWNER_RESULT, claim_id=token_b) == "WRITTEN"
        # ... e o zumbi nao reverte a linha finalizada.
        assert await repo.mark_failed(task_id, "A travou", 1, claim_id=token_a) == "CLAIM_LOST"

    final = await fetch_task(settings, task_id)
    assert final is not None
    assert final.status == TaskStatus.COMPLETED
    assert final.result == OWNER_RESULT
    assert final.error is None
    assert final.claimed_at is None
    assert final.claim_id is None


async def test_claim_lease_locks_a_live_claim_and_frees_a_stale_one(
    settings: Settings,
    services: None,
) -> None:
    """O lease distingue claim vivo (LOCKED) de claim orfao (reclamavel)."""
    task_id = uuid.uuid4()
    claim_id = uuid.uuid4()

    async with session_scope(settings) as session:
        repo = TaskRepository(session, settings)
        assert (
            await repo.claim_for_processing(task_id, EVENT_TYPE, {}, 1, claim_id=claim_id)
            == "CLAIMED"
        )
        # Claim ainda dentro do lease: a segunda entrega nao pode processar.
        assert (
            await repo.claim_for_processing(task_id, EVENT_TYPE, {}, 1, claim_id=uuid.uuid4())
            == "LOCKED"
        )

        # Backdating em vez de sleep: o claim passa a estar fora do lease.
        await _expire_claim(session, settings, task_id)

        reclaim_id = uuid.uuid4()
        assert (
            await repo.claim_for_processing(task_id, EVENT_TYPE, {}, 2, claim_id=reclaim_id)
            == "CLAIMED"
        )

        # COMPLETED nunca volta a ser reclamavel, lease ou nao.
        assert await repo.mark_completed(task_id, {"done": True}, claim_id=reclaim_id) == "WRITTEN"
        assert (
            await repo.claim_for_processing(task_id, EVENT_TYPE, {}, 3, claim_id=uuid.uuid4())
            == "ALREADY_COMPLETED"
        )

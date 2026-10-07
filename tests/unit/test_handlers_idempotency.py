"""Garantia de idempotencia: duas entregas do mesmo task_id, um unico efeito."""

import uuid

import pytest

from app.core.config import Settings
from app.core.constants import FORCE_FAILURE_KEY
from app.core.exceptions import TaskProcessingError
from app.db.models import TaskStatus
from app.worker.handlers import TaskMessage, handle_task, simulate_processing
from tests.fakes import CountingProcessor, FakeTaskRepository


def _message(task_id: uuid.UUID, **payload: object) -> TaskMessage:
    return TaskMessage(task_id=task_id, event_type="demo", payload=dict(payload))


async def test_duplicate_delivery_runs_the_side_effect_once(
    fake_repository: FakeTaskRepository,
) -> None:
    task_id = uuid.uuid4()
    message = _message(task_id)
    processor = CountingProcessor(result={"ok": True})

    first = await handle_task(
        message, fake_repository, attempts=1, processor=processor, claim_id=uuid.uuid4()
    )
    second = await handle_task(
        message, fake_repository, attempts=2, processor=processor, claim_id=uuid.uuid4()
    )

    assert first == "PROCESSED"
    assert second == "SKIPPED_DUPLICATE"
    assert processor.count == 1
    assert len(fake_repository.rows) == 1
    row = fake_repository.rows[task_id]
    assert row.status == TaskStatus.COMPLETED
    assert row.result == {"ok": True}


async def test_duplicate_delivery_keeps_the_first_attempts_value(
    fake_repository: FakeTaskRepository,
) -> None:
    task_id = uuid.uuid4()
    message = _message(task_id)
    processor = CountingProcessor()

    await handle_task(
        message, fake_repository, attempts=1, processor=processor, claim_id=uuid.uuid4()
    )
    await handle_task(
        message, fake_repository, attempts=4, processor=processor, claim_id=uuid.uuid4()
    )

    # O short-circuit acontece antes de qualquer escrita: a linha nao e tocada.
    assert fake_repository.rows[task_id].attempts == 1


async def test_processing_failure_propagates_and_does_not_complete(
    fake_repository: FakeTaskRepository,
) -> None:
    task_id = uuid.uuid4()
    processor = CountingProcessor(always_fail=True)

    with pytest.raises(TaskProcessingError):
        await handle_task(
            _message(task_id),
            fake_repository,
            attempts=1,
            processor=processor,
            claim_id=uuid.uuid4(),
        )

    assert processor.count == 1
    assert fake_repository.rows[task_id].status == TaskStatus.PROCESSING
    assert fake_repository.rows[task_id].result is None


async def test_failed_task_is_processed_on_the_next_delivery(
    fake_repository: FakeTaskRepository,
) -> None:
    """Linha nao-COMPLETED pode ser reclamada: e assim que a retentativa funciona."""
    task_id = uuid.uuid4()
    processor = CountingProcessor(fail_times=1)
    claim_id = uuid.uuid4()

    with pytest.raises(TaskProcessingError):
        await handle_task(
            _message(task_id),
            fake_repository,
            attempts=1,
            processor=processor,
            claim_id=claim_id,
        )
    # O que `_route_failure` faz no fluxo real: grava FAILED (com o MESMO
    # token do claim desta entrega) e libera o claim.
    assert (
        await fake_repository.mark_failed(
            task_id, "forced failure on attempt 1", attempts=1, claim_id=claim_id
        )
        == "WRITTEN"
    )

    outcome = await handle_task(
        _message(task_id),
        fake_repository,
        attempts=2,
        processor=processor,
        claim_id=uuid.uuid4(),
    )

    assert outcome == "PROCESSED"
    assert processor.count == 2
    assert fake_repository.rows[task_id].status == TaskStatus.COMPLETED
    assert fake_repository.rows[task_id].attempts == 2


async def test_concurrent_delivery_is_skipped_while_the_claim_lease_is_alive(
    fake_repository: FakeTaskRepository,
) -> None:
    """Claim vivo de outro consumidor: a segunda entrega nao executa nada."""
    task_id = uuid.uuid4()
    processor = CountingProcessor()

    # Claim do "outro consumidor": a linha fica PROCESSING com lease fresco.
    assert (
        await fake_repository.claim_for_processing(task_id, "demo", {}, 1, claim_id=uuid.uuid4())
        == "CLAIMED"
    )

    outcome = await handle_task(
        _message(task_id),
        fake_repository,
        attempts=1,
        processor=processor,
        claim_id=uuid.uuid4(),
    )

    assert outcome == "SKIPPED_CONCURRENT"
    assert processor.count == 0
    assert fake_repository.rows[task_id].status == TaskStatus.PROCESSING
    assert fake_repository.rows[task_id].result is None


async def test_stale_claim_is_reclaimed_after_the_lease_expires(
    fake_repository: FakeTaskRepository,
) -> None:
    """Claim obsoleto (worker morto) nao bloqueia a task para sempre."""
    task_id = uuid.uuid4()
    processor = CountingProcessor()
    await fake_repository.claim_for_processing(task_id, "demo", {}, 1, claim_id=uuid.uuid4())

    fake_repository.expire_claim(task_id)
    outcome = await handle_task(
        _message(task_id),
        fake_repository,
        attempts=2,
        processor=processor,
        claim_id=uuid.uuid4(),
    )

    assert outcome == "PROCESSED"
    assert processor.count == 1
    assert fake_repository.rows[task_id].status == TaskStatus.COMPLETED


async def test_simulate_processing_fails_when_payload_forces_failure(
    settings: Settings,
) -> None:
    with pytest.raises(TaskProcessingError):
        await simulate_processing(
            uuid.uuid4(),
            "demo",
            {FORCE_FAILURE_KEY: True},
            settings=settings,
        )


async def test_simulate_processing_echoes_the_payload(settings: Settings) -> None:
    payload = {"order_id": 7}
    result = await simulate_processing(uuid.uuid4(), "demo", payload, settings=settings)
    assert result["echo"] == payload
    assert result["event_type"] == "demo"
    assert result["processed_at"]

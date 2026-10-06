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

    first = await handle_task(message, fake_repository, attempts=1, processor=processor)
    second = await handle_task(message, fake_repository, attempts=2, processor=processor)

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

    await handle_task(message, fake_repository, attempts=1, processor=processor)
    await handle_task(message, fake_repository, attempts=4, processor=processor)

    # O short-circuit acontece antes de qualquer escrita: a linha nao e tocada.
    assert fake_repository.rows[task_id].attempts == 1


async def test_processing_failure_propagates_and_does_not_complete(
    fake_repository: FakeTaskRepository,
) -> None:
    task_id = uuid.uuid4()
    processor = CountingProcessor(always_fail=True)

    with pytest.raises(TaskProcessingError):
        await handle_task(_message(task_id), fake_repository, attempts=1, processor=processor)

    assert processor.count == 1
    assert fake_repository.rows[task_id].status == TaskStatus.PROCESSING
    assert fake_repository.rows[task_id].result is None


async def test_failed_task_is_processed_on_the_next_delivery(
    fake_repository: FakeTaskRepository,
) -> None:
    """Linha nao-COMPLETED pode ser reclamada: e assim que a retentativa funciona."""
    task_id = uuid.uuid4()
    processor = CountingProcessor(fail_times=1)

    with pytest.raises(TaskProcessingError):
        await handle_task(_message(task_id), fake_repository, attempts=1, processor=processor)
    outcome = await handle_task(
        _message(task_id), fake_repository, attempts=2, processor=processor
    )

    assert outcome == "PROCESSED"
    assert processor.count == 2
    assert fake_repository.rows[task_id].status == TaskStatus.COMPLETED
    assert fake_repository.rows[task_id].attempts == 2


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

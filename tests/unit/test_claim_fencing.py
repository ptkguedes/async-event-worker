"""Fencing das report-backs: um worker zumbi nao sobrescreve a linha viva.

O cenario do defeito: o worker A reclama a task, trava, o lease vence e o
worker B reclama a MESMA linha. Quando A acorda e reporta de volta, a escrita
dele precisa casar ZERO linhas -- senao A sobrescreve o estado de B, e um
`mark_failed` atrasado chega a transformar um COMPLETED em FAILED reclamavel.

Um predicado de lease (`status = 'PROCESSING' AND claimed_at > now() - lease`)
nao resolve: depois do re-claim de B o `claimed_at` esta fresco de novo e a
escrita de A passaria. O que resolve e o TOKEN (`claim_id`): a escrita casa
pela identidade do claim que o escritor observou, e basta `TA <> TB`.
"""

import logging
import uuid

import pytest

from app.db.models import TaskStatus
from app.worker.handlers import TaskMessage, handle_task
from tests.fakes import CountingProcessor, FakeTaskRepository

EVENT_TYPE = "demo"
RESULT_A = {"written_by": "A"}
RESULT_B = {"written_by": "B"}


def _message(task_id: uuid.UUID) -> TaskMessage:
    return TaskMessage(task_id=task_id, event_type=EVENT_TYPE, payload={})


async def test_zombie_report_back_cannot_overwrite_the_new_owner(
    fake_repository: FakeTaskRepository,
) -> None:
    """Roteiro completo do zumbi: as duas escritas de A sao rejeitadas."""
    task_id = uuid.uuid4()
    token_a = uuid.uuid4()
    token_b = uuid.uuid4()

    # t0: A reclama a task.
    assert (
        await fake_repository.claim_for_processing(task_id, EVENT_TYPE, {}, 1, claim_id=token_a)
        == "CLAIMED"
    )

    # t1/t2: A travou, o lease venceu e B reclama a linha orfa.
    fake_repository.expire_claim(task_id)
    assert (
        await fake_repository.claim_for_processing(task_id, EVENT_TYPE, {}, 2, claim_id=token_b)
        == "CLAIMED"
    )

    # t3/t4: A acorda. NENHUMA das duas report-backs dele pode casar a linha.
    assert await fake_repository.mark_completed(task_id, RESULT_A, claim_id=token_a) == "CLAIM_LOST"
    assert (
        await fake_repository.mark_failed(task_id, "A travou", attempts=1, claim_id=token_a)
        == "CLAIM_LOST"
    )

    # A linha segue refletindo o claim de B, intacta.
    row = fake_repository.rows[task_id]
    assert row.status == TaskStatus.PROCESSING
    assert row.claim_id == token_b
    assert row.attempts == 2
    assert row.result is None
    assert row.error is None


async def test_the_live_owner_still_writes_and_the_zombie_cannot_revert_it(
    fake_repository: FakeTaskRepository,
) -> None:
    """B conclui; depois disso o zumbi nao reverte o COMPLETED para FAILED."""
    task_id = uuid.uuid4()
    token_a = uuid.uuid4()
    token_b = uuid.uuid4()

    await fake_repository.claim_for_processing(task_id, EVENT_TYPE, {}, 1, claim_id=token_a)
    fake_repository.expire_claim(task_id)
    await fake_repository.claim_for_processing(task_id, EVENT_TYPE, {}, 2, claim_id=token_b)

    # t5: o dono vivo escreve normalmente e libera o token.
    assert await fake_repository.mark_completed(task_id, RESULT_B, claim_id=token_b) == "WRITTEN"
    row = fake_repository.rows[task_id]
    assert row.status == TaskStatus.COMPLETED
    assert row.result == RESULT_B
    assert row.claimed_at is None
    assert row.claim_id is None

    # t6: o zumbi tenta registrar a falha dele sobre a linha ja finalizada.
    assert (
        await fake_repository.mark_failed(task_id, "A travou", attempts=1, claim_id=token_a)
        == "CLAIM_LOST"
    )
    assert row.status == TaskStatus.COMPLETED
    assert row.result == RESULT_B
    assert row.error is None


async def test_report_back_is_at_most_once_for_the_same_token(
    fake_repository: FakeTaskRepository,
) -> None:
    """A report-back zera o token, portanto repeti-la nao casa de novo."""
    task_id = uuid.uuid4()
    claim_id = uuid.uuid4()

    await fake_repository.claim_for_processing(task_id, EVENT_TYPE, {}, 1, claim_id=claim_id)

    assert await fake_repository.mark_completed(task_id, RESULT_B, claim_id=claim_id) == "WRITTEN"
    assert (
        await fake_repository.mark_completed(task_id, RESULT_A, claim_id=claim_id) == "CLAIM_LOST"
    )
    assert fake_repository.rows[task_id].result == RESULT_B


async def test_report_back_on_an_absent_row_is_missing(
    fake_repository: FakeTaskRepository,
) -> None:
    """Sem linha, ninguem detem a task: o chamador precisa seguir no retry."""
    task_id = uuid.uuid4()

    assert (
        await fake_repository.mark_failed(task_id, "boom", attempts=1, claim_id=uuid.uuid4())
        == "MISSING"
    )
    assert (
        await fake_repository.mark_completed(task_id, RESULT_A, claim_id=uuid.uuid4()) == "MISSING"
    )


async def test_handle_task_reports_the_lost_claim_instead_of_lying(
    fake_repository: FakeTaskRepository,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Claim perdido durante o processamento: outcome proprio e WARNING.

    O efeito colateral ja rodou (o fencing protege a LINHA, nao desfaz o
    efeito), mas esta entrega NAO pode declarar "task processed" nem deixar a
    rejeicao passar em silencio.
    """
    task_id = uuid.uuid4()
    token_a = uuid.uuid4()
    token_b = uuid.uuid4()
    processor = CountingProcessor(result=RESULT_A)

    async def _steal_the_claim(
        inner_task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, object],
    ) -> dict[str, object]:
        """Enquanto A processa, o lease vence e B reclama a linha."""
        fake_repository.expire_claim(inner_task_id)
        await fake_repository.claim_for_processing(
            inner_task_id, event_type, payload, 2, claim_id=token_b
        )
        return await processor(inner_task_id, event_type, payload)

    with caplog.at_level(logging.WARNING):
        outcome = await handle_task(
            _message(task_id),
            fake_repository,
            attempts=1,
            processor=_steal_the_claim,
            claim_id=token_a,
        )

    assert outcome == "SKIPPED_CLAIM_LOST"
    assert processor.count == 1
    warnings = [r for r in caplog.records if r.message == "task claim lost before completion"]
    assert len(warnings) == 1
    assert warnings[0].levelno == logging.WARNING
    assert warnings[0].task_id == str(task_id)
    assert warnings[0].write_result == "CLAIM_LOST"

    # A linha segue sendo de B: nada do worker A foi gravado.
    row = fake_repository.rows[task_id]
    assert row.status == TaskStatus.PROCESSING
    assert row.claim_id == token_b
    assert row.result is None

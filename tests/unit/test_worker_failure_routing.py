"""Roteamento no broker quando o BANCO falha ou o claim esta em outro consumidor.

INJECAO DE FALHA: `session_scope` e substituido por um contexto que levanta, o
que reproduz "Postgres fora" sem derrubar o servico. O que estes testes provam e
que a decisao do lado do broker (nack para retry ou publish na DLX + ack)
acontece SEMPRE -- uma mensagem sem ack e sem nack seguraria um slot de prefetch
para sempre.

A segunda metade cobre o lado do broker quando a escrita de falha e REJEITADA
pelo fencing: "CLAIM_LOST" (outro consumidor detem a linha) recebe ack sem
nack e sem DLX, enquanto "MISSING" (ninguem detem a task) segue a politica de
retry/DLX normal.
"""

import json
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from sqlalchemy.exc import DBAPIError

from app.core.broker import BODY_EVENT_TYPE, BODY_PAYLOAD, BODY_TASK_ID
from app.core.config import Settings
from app.core.constants import (
    X_ATTEMPTS_HEADER,
    X_FAILURE_REASON_HEADER,
    X_RETRY_COUNT_HEADER,
)
from app.db.models import TaskStatus
from app.db.repository import FencedWriteResult
from app.worker.handlers import TaskMessage
from app.worker.main import _route_failure, on_message
from tests.fakes import FakeIncomingMessage, FakePublisher

ERROR_MESSAGE = "processing blew up"
PERSISTENCE_ERROR = "postgres is down"
LAST_RETRY_COUNT = 3


class _FencedWriteResultStub:
    """Resultado de um UPDATE ... RETURNING que casou a linha."""

    def __init__(self, task_id: uuid.UUID) -> None:
        self._task_id = task_id

    def scalar_one_or_none(self) -> uuid.UUID:
        return self._task_id


class _RecordingSession:
    """AsyncSession minima: registra os statements e os commits.

    O `execute` devolve a linha casada, isto e, a report-back vista como
    "WRITTEN": sem isso o repositorio cairia no SELECT de rotulagem e o
    caminho felix passaria a executar 2 statements.
    """

    def __init__(self, task_id: uuid.UUID) -> None:
        self.task_id = task_id
        self.statements: list[Any] = []
        self.commits = 0

    async def execute(self, statement: Any) -> _FencedWriteResultStub:
        self.statements.append(statement)
        return _FencedWriteResultStub(self.task_id)

    async def commit(self) -> None:
        self.commits += 1


@asynccontextmanager
async def _broken_session_scope(_settings: Settings | None = None) -> AsyncIterator[None]:
    """Postgres indisponivel: a sessao nem chega a ser aberta."""
    raise RuntimeError(PERSISTENCE_ERROR)
    yield None  # pragma: no cover -- inalcancavel, mantem a funcao um gerador


def _session_scope_of(task_id: uuid.UUID) -> Any:
    """`session_scope` substituto entregando uma _RecordingSession."""

    @asynccontextmanager
    async def _scope(_settings: Settings | None = None) -> AsyncIterator[Any]:
        yield _RecordingSession(task_id)

    return _scope


def _repository_writing(result: FencedWriteResult) -> Any:
    """TaskRepository substituto cuja report-back devolve `result`.

    E o resultado da escrita FENCED que decide o lado do broker, por isso ele
    e injetado direto em vez de ser montado com estado de banco.
    """

    class _Repository:
        def __init__(self, _session: Any, _settings: Settings | None = None) -> None:
            pass

        async def mark_failed(self, *_args: Any, **_kwargs: Any) -> FencedWriteResult:
            return result

    return _Repository


def _message(task_id: uuid.UUID) -> TaskMessage:
    return TaskMessage(task_id=task_id, event_type="demo", payload={})


def _incoming(task_id: uuid.UUID, settings: Settings) -> FakeIncomingMessage:
    body = json.dumps(
        {
            BODY_TASK_ID: str(task_id),
            BODY_EVENT_TYPE: "demo",
            BODY_PAYLOAD: {},
        }
    ).encode()
    return FakeIncomingMessage(
        body=body,
        exchange=settings.tasks_exchange_name,
        routing_key=settings.tasks_routing_key,
    )


async def test_persistence_failure_still_nacks_for_retry(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Banco fora e retentativas disponiveis: a mensagem segue para o retry."""
    monkeypatch.setattr("app.worker.main.session_scope", _broken_session_scope)
    task_id = uuid.uuid4()
    message = _incoming(task_id, settings)

    with caplog.at_level(logging.ERROR):
        await _route_failure(
            message,
            _message(task_id),
            retry_count=0,
            error=ERROR_MESSAGE,
            settings=settings,
            publisher=fake_publisher,
            claim_id=uuid.uuid4(),
        )

    assert message.nacks == [False]
    assert message.acks == 0
    assert fake_publisher.dlx_messages == []

    persistence_errors = [
        r for r in caplog.records if r.message == "failed to persist task failure"
    ]
    assert len(persistence_errors) == 1
    assert persistence_errors[0].levelno == logging.ERROR
    assert persistence_errors[0].task_id == str(task_id)
    assert PERSISTENCE_ERROR in persistence_errors[0].error


async def test_persistence_failure_still_dead_letters_on_the_last_attempt(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Banco fora e retentativas esgotadas: a mensagem ainda chega na DLX."""
    monkeypatch.setattr("app.worker.main.session_scope", _broken_session_scope)
    task_id = uuid.uuid4()
    message = _incoming(task_id, settings)

    await _route_failure(
        message,
        _message(task_id),
        retry_count=LAST_RETRY_COUNT,
        error=ERROR_MESSAGE,
        settings=settings,
        publisher=fake_publisher,
        claim_id=uuid.uuid4(),
    )

    assert message.acks == 1
    assert message.nacks == []
    assert len(fake_publisher.dlx_messages) == 1
    dead = fake_publisher.dlx_messages[0]
    assert dead.decoded[BODY_TASK_ID] == str(task_id)
    assert dead.headers[X_RETRY_COUNT_HEADER] == LAST_RETRY_COUNT
    assert dead.headers[X_ATTEMPTS_HEADER] == LAST_RETRY_COUNT + 1
    assert dead.headers[X_FAILURE_REASON_HEADER] == ERROR_MESSAGE


async def test_failure_path_persists_and_nacks_when_the_database_is_healthy(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Caminho felix do roteamento de falha: grava FAILED e da nack, como antes."""
    task_id = uuid.uuid4()
    claim_id = uuid.uuid4()
    session = _RecordingSession(task_id)

    @asynccontextmanager
    async def _session_scope(_settings: Settings | None = None) -> AsyncIterator[Any]:
        yield session

    monkeypatch.setattr("app.worker.main.session_scope", _session_scope)
    message = _incoming(task_id, settings)

    with caplog.at_level(logging.ERROR):
        await _route_failure(
            message,
            _message(task_id),
            retry_count=1,
            error=ERROR_MESSAGE,
            settings=settings,
            publisher=fake_publisher,
            claim_id=claim_id,
        )

    assert len(session.statements) == 1
    values = session.statements[0].compile().params
    assert values["status"] == TaskStatus.FAILED
    assert values["attempts"] == 2
    assert values["error"] == ERROR_MESSAGE
    assert values["claimed_at"] is None
    # O token vai no WHERE (fencing) e e zerado no SET.
    assert values["claim_id_1"] == claim_id
    assert values["claim_id"] is None
    assert message.nacks == [False]
    assert not [r for r in caplog.records if r.message == "failed to persist task failure"]


async def test_concurrent_claim_nacks_without_writing_the_failure(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claim de outro consumidor nao e falha: nack sem tocar no banco."""
    task_id = uuid.uuid4()
    session = _RecordingSession(task_id)

    @asynccontextmanager
    async def _session_scope(_settings: Settings | None = None) -> AsyncIterator[Any]:
        yield session

    async def _skipped_concurrent(*_args: Any, **_kwargs: Any) -> str:
        return "SKIPPED_CONCURRENT"

    monkeypatch.setattr("app.worker.main.session_scope", _session_scope)
    monkeypatch.setattr("app.worker.main.handle_task", _skipped_concurrent)
    message = _incoming(task_id, settings)

    await on_message(
        message,
        settings=settings,
        publisher=fake_publisher,
        processor=None,
    )

    assert message.nacks == [False]
    assert message.acks == 0
    assert session.statements == []
    assert fake_publisher.dlx_messages == []


async def test_concurrent_claim_dead_letters_when_the_retries_are_exhausted(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claim ainda ativo no ultimo hop: DLX em vez de perda silenciosa."""
    task_id = uuid.uuid4()

    @asynccontextmanager
    async def _session_scope(_settings: Settings | None = None) -> AsyncIterator[Any]:
        yield _RecordingSession(task_id)

    async def _skipped_concurrent(*_args: Any, **_kwargs: Any) -> str:
        return "SKIPPED_CONCURRENT"

    monkeypatch.setattr("app.worker.main.session_scope", _session_scope)
    monkeypatch.setattr("app.worker.main.handle_task", _skipped_concurrent)
    message = _incoming(task_id, settings)
    message.headers = {
        "x-death": [
            {"queue": settings.tasks_queue_name, "reason": "rejected", "count": LAST_RETRY_COUNT}
        ]
    }

    await on_message(
        message,
        settings=settings,
        publisher=fake_publisher,
        processor=None,
    )

    assert message.acks == 1
    assert len(fake_publisher.dlx_messages) == 1
    assert fake_publisher.dlx_messages[0].headers[X_ATTEMPTS_HEADER] == LAST_RETRY_COUNT + 1


async def test_lost_claim_acks_without_retrying_or_dead_lettering(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Escrita de falha rejeitada pelo fencing: ack, sem nack e sem DLX.

    "CLAIM_LOST" significa que outro consumidor detem a linha AGORA e tem a
    propria entrega para resolver a task. Nackear aqui seria uma retentativa
    duplicada e publicar na DLX seria um dead letter espurio -- os dois efeitos
    que o worker zumbi nao pode causar.
    """
    task_id = uuid.uuid4()
    monkeypatch.setattr("app.worker.main.session_scope", _session_scope_of(task_id))
    monkeypatch.setattr("app.worker.main.TaskRepository", _repository_writing("CLAIM_LOST"))
    message = _incoming(task_id, settings)

    with caplog.at_level(logging.WARNING):
        await _route_failure(
            message,
            _message(task_id),
            retry_count=0,
            error=ERROR_MESSAGE,
            settings=settings,
            publisher=fake_publisher,
            claim_id=uuid.uuid4(),
        )

    assert message.acks == 1
    assert message.nacks == []
    assert fake_publisher.dlx_messages == []
    warnings = [
        r for r in caplog.records if r.message == "task claim lost before the failure report"
    ]
    assert len(warnings) == 1
    assert warnings[0].levelno == logging.WARNING
    assert warnings[0].task_id == str(task_id)


async def test_missing_row_keeps_following_the_retry_policy(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rotulo MISSING = ninguem detem a task: ack perderia a mensagem, logo nack."""
    task_id = uuid.uuid4()
    monkeypatch.setattr("app.worker.main.session_scope", _session_scope_of(task_id))
    monkeypatch.setattr("app.worker.main.TaskRepository", _repository_writing("MISSING"))
    message = _incoming(task_id, settings)

    await _route_failure(
        message,
        _message(task_id),
        retry_count=0,
        error=ERROR_MESSAGE,
        settings=settings,
        publisher=fake_publisher,
        claim_id=uuid.uuid4(),
    )

    assert message.nacks == [False]
    assert message.acks == 0
    assert fake_publisher.dlx_messages == []


async def test_missing_row_still_dead_letters_on_the_last_attempt(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cadeia de retry/DLX nao muda por causa do fencing."""
    task_id = uuid.uuid4()
    monkeypatch.setattr("app.worker.main.session_scope", _session_scope_of(task_id))
    monkeypatch.setattr("app.worker.main.TaskRepository", _repository_writing("MISSING"))
    message = _incoming(task_id, settings)

    await _route_failure(
        message,
        _message(task_id),
        retry_count=LAST_RETRY_COUNT,
        error=ERROR_MESSAGE,
        settings=settings,
        publisher=fake_publisher,
        claim_id=uuid.uuid4(),
    )

    assert message.acks == 1
    assert message.nacks == []
    assert len(fake_publisher.dlx_messages) == 1
    assert fake_publisher.dlx_messages[0].headers[X_ATTEMPTS_HEADER] == LAST_RETRY_COUNT + 1


async def test_claim_lost_outcome_is_acked_by_the_final_path(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Outcome "SKIPPED_CLAIM_LOST" cai no ack final: nada de nack nem DLX."""
    task_id = uuid.uuid4()
    session = _RecordingSession(task_id)

    @asynccontextmanager
    async def _session_scope(_settings: Settings | None = None) -> AsyncIterator[Any]:
        yield session

    async def _claim_lost(*_args: Any, **_kwargs: Any) -> str:
        return "SKIPPED_CLAIM_LOST"

    monkeypatch.setattr("app.worker.main.session_scope", _session_scope)
    monkeypatch.setattr("app.worker.main.handle_task", _claim_lost)
    message = _incoming(task_id, settings)

    await on_message(
        message,
        settings=settings,
        publisher=fake_publisher,
        processor=None,
    )

    assert message.acks == 1
    assert message.nacks == []
    assert fake_publisher.dlx_messages == []


async def test_a_hanging_persistence_write_does_not_hang_the_message(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Consequencia dos timeouts do banco no consumidor.

    Um Postgres que aceita o TCP e nunca responde faz o asyncpg levantar
    `TimeoutError` (e nao pendurar para sempre). A contencao de `_persist_failure`
    transforma isso em `nack`: a mensagem e dead-lettered pelo hop de retry em
    vez de segurar um slot de prefetch, e o worker continua consumindo.
    """

    @asynccontextmanager
    async def _hanging_session_scope(
        _settings: Settings | None = None,
    ) -> AsyncIterator[None]:
        raise TimeoutError
        yield None  # pragma: no cover -- inalcancavel, mantem a funcao um gerador

    monkeypatch.setattr("app.worker.main.session_scope", _hanging_session_scope)
    task_id = uuid.uuid4()
    message = _incoming(task_id, settings)

    with caplog.at_level(logging.ERROR):
        await _route_failure(
            message,
            _message(task_id),
            retry_count=0,
            error=ERROR_MESSAGE,
            settings=settings,
            publisher=fake_publisher,
            claim_id=uuid.uuid4(),
        )

    assert message.nacks == [False]
    assert message.settled == 1
    errors = [r for r in caplog.records if r.message == "failed to persist task failure"]
    assert len(errors) == 1
    assert "TimeoutError" in errors[0].error


async def test_a_statement_timeout_inside_handle_task_routes_to_retry(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timeout de statement dentro de `handle_task` desce pelo retry/DLX.

    `DBAPIError` (o que o `statement_timeout` do Postgres produz) nao e
    `TaskProcessingError`: cai no `except Exception` do `on_message`, que segue
    a mesma politica de retentativa -- a mensagem nunca fica sem ack/nack.
    """
    task_id = uuid.uuid4()
    monkeypatch.setattr("app.worker.main.session_scope", _session_scope_of(task_id))
    monkeypatch.setattr("app.worker.main.TaskRepository", _repository_writing("WRITTEN"))

    async def _statement_timeout(*_args: Any, **_kwargs: Any) -> str:
        raise DBAPIError("UPDATE tasks", {}, Exception("canceling statement due to timeout"))

    monkeypatch.setattr("app.worker.main.handle_task", _statement_timeout)
    message = _incoming(task_id, settings)

    await on_message(
        message,
        settings=settings,
        publisher=fake_publisher,
        processor=None,
    )

    assert message.nacks == [False]
    assert message.acks == 0

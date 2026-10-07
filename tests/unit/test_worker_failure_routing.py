"""Roteamento no broker quando o BANCO falha ou o claim esta em outro consumidor.

INJECAO DE FALHA: `session_scope` e substituido por um contexto que levanta, o
que reproduz "Postgres fora" sem derrubar o servico. O que estes testes provam e
que a decisao do lado do broker (nack para retry ou publish na DLX + ack)
acontece SEMPRE -- uma mensagem sem ack e sem nack seguraria um slot de prefetch
para sempre.
"""

import json
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from app.core.broker import BODY_EVENT_TYPE, BODY_PAYLOAD, BODY_TASK_ID
from app.core.config import Settings
from app.core.constants import (
    X_ATTEMPTS_HEADER,
    X_FAILURE_REASON_HEADER,
    X_RETRY_COUNT_HEADER,
)
from app.db.models import TaskStatus
from app.worker.handlers import TaskMessage
from app.worker.main import _route_failure, on_message
from tests.fakes import FakeIncomingMessage, FakePublisher

ERROR_MESSAGE = "processing blew up"
PERSISTENCE_ERROR = "postgres is down"
LAST_RETRY_COUNT = 3


class _RecordingSession:
    """AsyncSession minima: registra os statements e os commits."""

    def __init__(self) -> None:
        self.statements: list[Any] = []
        self.commits = 0

    async def execute(self, statement: Any) -> None:
        self.statements.append(statement)

    async def commit(self) -> None:
        self.commits += 1


@asynccontextmanager
async def _broken_session_scope(_settings: Settings | None = None) -> AsyncIterator[None]:
    """Postgres indisponivel: a sessao nem chega a ser aberta."""
    raise RuntimeError(PERSISTENCE_ERROR)
    yield None  # pragma: no cover -- inalcancavel, mantem a funcao um gerador


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
    session = _RecordingSession()

    @asynccontextmanager
    async def _session_scope(_settings: Settings | None = None) -> AsyncIterator[Any]:
        yield session

    monkeypatch.setattr("app.worker.main.session_scope", _session_scope)
    task_id = uuid.uuid4()
    message = _incoming(task_id, settings)

    with caplog.at_level(logging.ERROR):
        await _route_failure(
            message,
            _message(task_id),
            retry_count=1,
            error=ERROR_MESSAGE,
            settings=settings,
            publisher=fake_publisher,
        )

    assert len(session.statements) == 1
    values = session.statements[0].compile().params
    assert values["status"] == TaskStatus.FAILED
    assert values["attempts"] == 2
    assert values["error"] == ERROR_MESSAGE
    assert values["claimed_at"] is None
    assert message.nacks == [False]
    assert not [r for r in caplog.records if r.message == "failed to persist task failure"]


async def test_concurrent_claim_nacks_without_writing_the_failure(
    settings: Settings,
    fake_publisher: FakePublisher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claim de outro consumidor nao e falha: nack sem tocar no banco."""
    session = _RecordingSession()

    @asynccontextmanager
    async def _session_scope(_settings: Settings | None = None) -> AsyncIterator[Any]:
        yield session

    async def _skipped_concurrent(*_args: Any, **_kwargs: Any) -> str:
        return "SKIPPED_CONCURRENT"

    monkeypatch.setattr("app.worker.main.session_scope", _session_scope)
    monkeypatch.setattr("app.worker.main.handle_task", _skipped_concurrent)
    task_id = uuid.uuid4()
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

    @asynccontextmanager
    async def _session_scope(_settings: Settings | None = None) -> AsyncIterator[Any]:
        yield _RecordingSession()

    async def _skipped_concurrent(*_args: Any, **_kwargs: Any) -> str:
        return "SKIPPED_CONCURRENT"

    monkeypatch.setattr("app.worker.main.session_scope", _session_scope)
    monkeypatch.setattr("app.worker.main.handle_task", _skipped_concurrent)
    task_id = uuid.uuid4()
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

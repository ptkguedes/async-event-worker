"""Dependencias injetaveis da API.

Os testes substituem estas funcoes via `app.dependency_overrides`, por isso elas
sao o unico ponto de acesso ao broker e ao banco dentro dos endpoints.
"""

from collections.abc import AsyncIterator

from fastapi import HTTPException, Request, status

from app.core.broker import BrokerConnection, TaskPublisher
from app.core.config import Settings, get_settings
from app.db.repository import TaskRepository, TaskRepositoryProtocol
from app.db.session import session_scope


def get_settings_dep() -> Settings:
    """Settings da aplicacao (instancia cacheada)."""
    return get_settings()


def get_publisher(request: Request) -> TaskPublisher:
    """Publisher criado no lifespan. 503 enquanto o broker nao estiver pronto."""
    publisher: TaskPublisher | None = getattr(request.app.state, "publisher", None)
    if publisher is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="broker publisher is not available",
        )
    return publisher


def get_broker(request: Request) -> BrokerConnection | None:
    """Conexao do broker guardada no lifespan (None quando ausente).

    Nao levanta: o healthcheck precisa poder reportar o componente degradado.
    """
    return getattr(request.app.state, "broker", None)


async def get_repository() -> AsyncIterator[TaskRepositoryProtocol]:
    """Uma sessao de banco por request, fechada no fim dela."""
    async with session_scope() as session:
        yield TaskRepository(session)

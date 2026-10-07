"""Engine assincrona, sessionmaker e helpers de ciclo de vida do banco."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings, get_settings

# Margem entre o teto do lado cliente e o do lado servidor, para que em
# operacao normal o erro venha do servidor (sqlstate 57014, mais informativo).
_COMMAND_TIMEOUT_MARGIN_SECONDS = 1.0

_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def build_connect_args(settings: Settings) -> dict[str, Any]:
    """connect_args do asyncpg: timeouts de conexao e de statement.

    As tres chaves vao para lugares diferentes de proposito:

    - `timeout` e o limite de ESTABELECIMENTO da conexao (parametro do
      `asyncpg.connect`); estourado, levanta `TimeoutError`.
    - `statement_timeout` e um GUC do Postgres, em MILISSEGUNDOS e como
      STRING, por isso vai dentro de `server_settings`; estourado, levanta
      `DBAPIError` com sqlstate 57014 e NAO invalida a conexao.
    - `command_timeout` e a metade do lado cliente: fecha o caso em que o
      servidor aceita o TCP e nunca responde, quando nenhum timeout do
      servidor chega a disparar.

    O dialeto asyncpg do SQLAlchemy repassa `connect_args` verbatim para
    `asyncpg.connect`, portanto uma chave errada falha ALTO (`TypeError`) em
    vez de ser ignorada em silencio.
    """
    return {
        "timeout": settings.db_connect_timeout_seconds,
        "command_timeout": (
            settings.db_statement_timeout_ms / 1000 + _COMMAND_TIMEOUT_MARGIN_SECONDS
        ),
        "server_settings": {"statement_timeout": str(settings.db_statement_timeout_ms)},
    }


def get_engine(settings: Settings | None = None) -> AsyncEngine:
    """Engine global (criada na primeira chamada).

    Os timeouts valem para a API e o worker, nao para as migrations: o
    `alembic/env.py` monta a propria engine com `async_engine_from_config` e
    nunca importa este modulo, logo um DDL longo roda com o `statement_timeout`
    default do servidor (`0`, desligado) e nao pode ser morto no meio.
    """
    global _engine
    if _engine is None:
        settings = settings or get_settings()
        _engine = create_async_engine(
            settings.database_url,
            echo=settings.db_echo,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_pre_ping=True,
            connect_args=build_connect_args(settings),
        )
    return _engine


def get_sessionmaker(settings: Settings | None = None) -> async_sessionmaker[AsyncSession]:
    """Fabrica de sessoes global.

    `expire_on_commit=False` mantem os objetos utilizaveis depois do commit.
    """
    global _sessionmaker
    if _sessionmaker is None:
        _sessionmaker = async_sessionmaker(
            bind=get_engine(settings),
            expire_on_commit=False,
            autoflush=False,
        )
    return _sessionmaker


async def dispose_engine() -> None:
    """Fecha o pool de conexoes e zera o estado global (chamado no shutdown)."""
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _sessionmaker = None


@asynccontextmanager
async def session_scope(
    settings: Settings | None = None,
) -> AsyncIterator[AsyncSession]:
    """Sessao transacional: commit no final, rollback em caso de excecao."""
    factory = get_sessionmaker(settings)
    session = factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def ping(settings: Settings | None = None) -> bool:
    """Executa SELECT 1; usado pelo healthcheck da API."""
    engine = get_engine(settings)
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
    return True

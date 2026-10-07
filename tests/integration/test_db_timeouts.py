"""Timeouts do banco contra o Postgres de verdade.

Dois riscos cobertos aqui. O primeiro e o `connect_args` ser aceito e NAO
vigorar: por isso o GUC e lido de volta do servidor e um statement
deliberadamente lento e executado. O segundo e o timeout matar uma migration no
meio de um DDL: por isso a engine e montada do MESMO jeito que o
`alembic/env.py` monta, para provar que la o `statement_timeout` continua `0`.
"""

import asyncio

import pytest
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_engine_from_config
from sqlalchemy.pool import NullPool

from app.core.config import Settings
from app.db.session import dispose_engine, get_engine
from tests.integration.conftest import PROJECT_ROOT

# Pequeno de proposito: a prova nao precisa fazer a suite esperar o default.
SHORT_STATEMENT_TIMEOUT_MS = 300
SLOW_STATEMENT_SECONDS = 2
STATEMENT_TIMEOUT_SQLSTATE = "57014"
ALEMBIC_STATEMENT_TIMEOUT = "0"


def _settings_with_timeout(settings: Settings, statement_timeout_ms: int) -> Settings:
    """Copia do settings da suite trocando so o timeout de statement."""
    return Settings(
        _env_file=None,
        database_url=settings.database_url,
        amqp_url=settings.amqp_url,
        db_statement_timeout_ms=statement_timeout_ms,
    )


async def test_the_configured_statement_timeout_reaches_the_server(
    settings: Settings,
    services: None,
) -> None:
    """O GUC lido de volta prova que o `connect_args` vigorou."""
    # A engine global e compartilhada e a fixture autouse ja a criou com o
    # settings da suite: descartar antes e depois para nao vazar esta.
    await dispose_engine()
    try:
        engine = get_engine(_settings_with_timeout(settings, SHORT_STATEMENT_TIMEOUT_MS))
        async with engine.connect() as connection:
            guc = (await connection.execute(text("show statement_timeout"))).scalar_one()
        assert guc == f"{SHORT_STATEMENT_TIMEOUT_MS}ms"
    finally:
        await dispose_engine()


async def test_a_slow_statement_is_cancelled_by_the_server(
    settings: Settings,
    services: None,
) -> None:
    """`pg_sleep` acima do timeout e cancelado, e a conexao segue utilizavel."""
    await dispose_engine()
    try:
        engine = get_engine(_settings_with_timeout(settings, SHORT_STATEMENT_TIMEOUT_MS))
        async with engine.connect() as connection:
            with pytest.raises(DBAPIError) as exc_info:
                await connection.execute(text(f"select pg_sleep({SLOW_STATEMENT_SECONDS})"))

        # O cancelamento vem do SERVIDOR: sqlstate 57014 (query_canceled). Nao
        # e `OperationalError` nem invalida a conexao.
        cause = exc_info.value.orig.__cause__
        assert getattr(cause, "sqlstate", None) == STATEMENT_TIMEOUT_SQLSTATE
        assert "statement timeout" in str(cause)
        assert exc_info.value.connection_invalidated is False

        # Prova de que o pool nao ficou envenenado pelo cancelamento.
        async with engine.connect() as connection:
            assert (await connection.execute(text("select 1"))).scalar_one() == 1
    finally:
        await dispose_engine()


async def test_the_statement_timeout_is_longer_than_the_suite_needs(
    settings: Settings,
    services: None,
) -> None:
    """O default nao estrangula a suite: o maior statement dela cabe folgado."""
    await dispose_engine()
    try:
        default_timeout_ms = Settings(_env_file=None).db_statement_timeout_ms
        engine = get_engine(_settings_with_timeout(settings, default_timeout_ms))
        loop = asyncio.get_running_loop()
        started = loop.time()
        async with engine.connect() as connection:
            await connection.execute(text("TRUNCATE TABLE tasks"))
        elapsed_ms = (loop.time() - started) * 1000
        assert elapsed_ms < default_timeout_ms
    finally:
        await dispose_engine()


async def test_alembic_runs_without_a_statement_timeout(
    settings: Settings,
    services: None,
) -> None:
    """Migration nao pode ser morta no meio de um DDL.

    A engine e montada exatamente como `alembic/env.py` monta (que nunca
    importa `app.db.session`): o GUC tem de continuar `0`, isto e, desligado.
    """
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    # O configparser do Alembic interpola "%": mesmo escape do env.py.
    config.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=NullPool,
    )
    try:
        async with connectable.connect() as connection:
            guc = (await connection.execute(text("show statement_timeout"))).scalar_one()
        assert guc == ALEMBIC_STATEMENT_TIMEOUT
    finally:
        await connectable.dispose()

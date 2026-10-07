"""Fixtures da suite de integracao (RabbitMQ e PostgreSQL de verdade).

Tudo aqui depende de servicos externos no ar, por isso todos os testes deste
diretorio recebem o marker `integration` automaticamente (ver
`pytest_collection_modifyitems`) e a suite default -- `python -m pytest -q` --
os desseleciona via `addopts = -m 'not integration'`.

As URLs vem de `TEST_DATABASE_URL` e `TEST_AMQP_URL` (defaults apontando para
localhost, como o `docker-compose.yml` publica as portas). A topologia usa
`topology_prefix="test_"`: sem o prefixo, o worker que roda no Compose
consumiria a mesma fila `tasks` e brigaria com o worker in-process da suite.
"""

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import aio_pika
import pytest
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage, AbstractQueue
from alembic import command
from alembic.config import Config
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import Settings, get_settings
from app.core.topology import DeclaredTopology, declare_topology
from app.db.models import Task
from app.db.session import dispose_engine, session_scope
from app.worker.main import run_worker
from tests.conftest import TEST_TOPOLOGY_PREFIX

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INTEGRATION_DIR = Path(__file__).resolve().parent

# Defaults de dev local: as portas que o docker-compose.yml publica no host.
DEFAULT_TEST_DATABASE_URL = "postgresql+asyncpg://app:app@localhost:5432/async_event_worker"
DEFAULT_TEST_AMQP_URL = "amqp://guest:guest@localhost:5672/"

CONNECT_TIMEOUT_SECONDS = 5.0
SKIP_HINT = "suba os servicos com: docker compose up -d postgres rabbitmq"

# TTL curto e sem delay de processamento para o fluxo de retry rodar rapido.
TEST_RETRY_TTL_MS = 500
TEST_PREFETCH_COUNT = 1

# Lease do claim na suite: 4x o TTL de teste e ordens de grandeza acima do
# processamento (que aqui e instantaneo), o que torna o teste concorrente
# estavel. Nenhum teste espera o lease expirar -- o caso obsoleto e provocado
# backdating o claimed_at com um UPDATE direto, deterministico e instantaneo.
TEST_CLAIM_LEASE_SECONDS = 2.0

WORKER_STARTUP_SECONDS = 0.5
WORKER_SHUTDOWN_TIMEOUT_SECONDS = 10.0
DEFAULT_WAIT_TIMEOUT_SECONDS = 10.0
POLL_INTERVAL_SECONDS = 0.1


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Aplica o marker `integration` a todo teste deste diretorio."""
    for item in items:
        if INTEGRATION_DIR in Path(str(item.path)).parents:
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def settings() -> Settings:
    """Settings da suite de integracao (sobrescreve a fixture unitaria)."""
    database_url = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    amqp_url = os.environ.get("TEST_AMQP_URL", DEFAULT_TEST_AMQP_URL)

    # O Alembic e o shutdown do worker leem get_settings() (Settings global):
    # exportar as URLs garante que esses caminhos tambem apontem para o host.
    os.environ["DATABASE_URL"] = database_url
    os.environ["AMQP_URL"] = amqp_url
    get_settings.cache_clear()

    return Settings(
        _env_file=None,
        database_url=database_url,
        amqp_url=amqp_url,
        topology_prefix=TEST_TOPOLOGY_PREFIX,
        retry_ttl_ms=TEST_RETRY_TTL_MS,
        processing_delay_seconds=0,
        worker_prefetch_count=TEST_PREFETCH_COUNT,
        claim_lease_seconds=TEST_CLAIM_LEASE_SECONDS,
    )


async def _check_postgres(settings: Settings) -> str | None:
    """Devolve a mensagem de erro, ou None quando o banco responde."""
    engine = create_async_engine(settings.database_url, poolclass=NullPool)

    async def _select_one() -> None:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    try:
        # Qualquer falha aqui (conexao, auth, timeout) significa "indisponivel".
        await asyncio.wait_for(_select_one(), timeout=CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:
        return f"PostgreSQL indisponivel em {settings.database_url}: {exc}"
    finally:
        await engine.dispose()
    return None


async def _check_rabbitmq(settings: Settings) -> str | None:
    """Devolve a mensagem de erro, ou None quando o broker responde."""
    try:
        # Qualquer falha aqui (conexao, auth, timeout) significa "indisponivel".
        connection = await asyncio.wait_for(
            aio_pika.connect(settings.amqp_url),
            timeout=CONNECT_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        return f"RabbitMQ indisponivel em {settings.amqp_url}: {exc}"
    await connection.close()
    return None


def _apply_migrations() -> None:
    """Aplica `alembic upgrade head` de verdade (valida a migration tambem)."""
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    command.upgrade(config, "head")


@pytest.fixture(scope="session")
def services(settings: Settings) -> None:
    """Exige Postgres e RabbitMQ no ar e deixa o schema na ultima migration.

    Sincrona de proposito: cada checagem roda em seu proprio `asyncio.run()`,
    sem prender conexoes a um event loop de escopo diferente do dos testes.
    """
    for check in (_check_postgres, _check_rabbitmq):
        error = asyncio.run(check(settings))
        if error is not None:
            pytest.skip(f"{error} -- {SKIP_HINT}")

    _apply_migrations()


@pytest.fixture(autouse=True)
async def truncate_tasks(settings: Settings, services: None) -> AsyncIterator[None]:
    """Zera a tabela `tasks` antes de cada teste e descarta a engine depois.

    O descarte importa: a engine e a sessionmaker de `app.db.session` sao
    globais e ficam presas ao event loop em que foram criadas -- cada teste tem
    o seu.
    """
    async with session_scope(settings) as session:
        await session.execute(text("TRUNCATE TABLE tasks"))
    try:
        yield
    finally:
        await dispose_engine()


@pytest.fixture
async def broker_channel(
    settings: Settings,
    services: None,
) -> AsyncIterator[AbstractChannel]:
    """Canal AMQP proprio do teste (independente do canal do worker)."""
    connection = await aio_pika.connect_robust(settings.amqp_url)
    channel = await connection.channel(publisher_confirms=True)
    try:
        yield channel
    finally:
        await channel.close()
        await connection.close()


@pytest.fixture
async def purged_topology(
    broker_channel: AbstractChannel,
    settings: Settings,
) -> DeclaredTopology:
    """Declara a topologia `test_` e esvazia as 3 filas antes do teste."""
    topology = await declare_topology(broker_channel, settings)
    for queue in (topology.tasks_queue, topology.retry_queue, topology.dlx_queue):
        await queue.purge()
    return topology


@pytest.fixture
async def running_worker(
    settings: Settings,
    purged_topology: DeclaredTopology,
) -> AsyncIterator[None]:
    """Roda o consumer in-process como asyncio.Task e para no teardown."""
    stop_event = asyncio.Event()
    worker = asyncio.create_task(run_worker(settings=settings, stop_event=stop_event))

    # Dar tempo ao consumidor de se registrar no broker antes de publicar.
    await asyncio.sleep(WORKER_STARTUP_SECONDS)
    if worker.done():
        worker.result()  # propaga a falha de inicializacao, se houver

    try:
        yield
    finally:
        stop_event.set()
        try:
            await asyncio.wait_for(worker, timeout=WORKER_SHUTDOWN_TIMEOUT_SECONDS)
        except TimeoutError:
            worker.cancel()


async def fetch_task(settings: Settings, task_id: uuid.UUID) -> Task | None:
    """Le a linha da task direto do banco."""
    async with session_scope(settings) as session:
        return (
            await session.execute(select(Task).where(Task.task_id == task_id))
        ).scalar_one_or_none()


async def count_tasks(settings: Settings, task_id: uuid.UUID) -> int:
    """Conta quantas linhas existem para a task_id (idempotencia => 1)."""
    async with session_scope(settings) as session:
        return (
            await session.execute(
                select(func.count()).select_from(Task).where(Task.task_id == task_id)
            )
        ).scalar_one()


async def wait_for_task(
    settings: Settings,
    task_id: uuid.UUID,
    predicate: Callable[[Task], bool],
    timeout: float = DEFAULT_WAIT_TIMEOUT_SECONDS,
) -> Task:
    """Faz polling na tabela `tasks` ate `predicate` ser satisfeita."""
    deadline = asyncio.get_running_loop().time() + timeout
    last: Task | None = None
    while asyncio.get_running_loop().time() < deadline:
        last = await fetch_task(settings, task_id)
        if last is not None and predicate(last):
            return last
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"timeout de {timeout}s esperando a task {task_id}; estado final: {last}")


async def queue_message_count(channel: AbstractChannel, queue_name: str) -> int:
    """Mensagens prontas na fila (declaracao passiva, nao altera a topologia)."""
    queue = await channel.declare_queue(queue_name, passive=True)
    return queue.declaration_result.message_count


async def wait_for_empty_queue(
    channel: AbstractChannel,
    queue_name: str,
    timeout: float = DEFAULT_WAIT_TIMEOUT_SECONDS,
) -> None:
    """Espera a fila drenar (o worker ja deu ack em tudo)."""
    deadline = asyncio.get_running_loop().time() + timeout
    count = await queue_message_count(channel, queue_name)
    while count > 0 and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        count = await queue_message_count(channel, queue_name)
    assert count == 0, f"a fila {queue_name} ainda tem {count} mensagem(ns)"


async def wait_for_message(
    queue: AbstractQueue,
    timeout: float = DEFAULT_WAIT_TIMEOUT_SECONDS,
) -> AbstractIncomingMessage:
    """Faz basic_get na fila ate uma mensagem aparecer (consome com no_ack)."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        message = await queue.get(no_ack=True, fail=False)
        if message is not None:
            return message
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"timeout de {timeout}s esperando mensagem na fila {queue.name}")

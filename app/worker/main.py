"""Entrypoint do worker assincrono (consumer AMQP).

`python -m app.worker.main` abre a conexao com o broker, declara a MESMA
topologia usada pela API e consome a fila de trabalho com ack manual.

Por que ack manual (sem `message.process()`): o destino de uma mensagem que
falhou depende da contagem de retentativas. Em caso de falha o worker da
`nack(requeue=False)` -- e a DLX NATIVA da fila `tasks` que leva a mensagem para
`tasks.retry`, onde o `x-message-ttl` cumpre o atraso e a devolve para `tasks`.
Nao existe loop de retry em codigo de aplicacao. Esgotadas as retentativas, o
corpo original e publicado explicitamente em `dlx_tasks` e so entao a mensagem
original recebe ack.
"""

import asyncio
import json
import logging
import signal
import uuid
from functools import partial

from aio_pika.abc import AbstractIncomingMessage

from app.core.broker import BrokerConnection, TaskPublisher
from app.core.config import Settings, get_settings
from app.core.constants import (
    X_ATTEMPTS_HEADER,
    X_FAILURE_REASON_HEADER,
    X_RETRY_COUNT_HEADER,
)
from app.core.exceptions import TaskProcessingError
from app.core.logging import configure_logging
from app.db.repository import FencedWriteResult, TaskRepository
from app.db.session import dispose_engine, get_engine, session_scope
from app.worker.handlers import (
    Processor,
    TaskMessage,
    handle_task,
    parse_task_message,
    simulate_processing,
)
from app.worker.retry import decide, retry_count_from_headers

logger = logging.getLogger(__name__)


async def _send_to_dead_letter(
    message: AbstractIncomingMessage,
    headers: dict[str, object],
    publisher: TaskPublisher,
) -> None:
    """Publica o corpo original na dead letter exchange e da ack na mensagem."""
    await publisher.publish_to_dlx(
        body=message.body,
        headers=headers,
        original_exchange=message.exchange or "",
        original_routing_key=message.routing_key or "",
    )
    await message.ack()


async def _persist_failure(
    task_message: TaskMessage,
    error: str,
    attempts: int,
    settings: Settings,
    claim_id: uuid.UUID,
) -> FencedWriteResult | None:
    """Grava a falha no banco SEM deixar o erro de persistencia escapar.

    CONTENCAO DELIBERADA: se o Postgres estiver fora, a excecao desta escrita
    nao pode impedir o `nack` (retry) nem o `publish` na DLX. Sem ack e sem
    nack a mensagem ficaria segurando um slot de prefetch para sempre, e o
    consumidor degradaria ate parar.

    Devolve o resultado da escrita fenced, ou `None` quando a escrita levantou
    e foi contida -- o chamador usa isso para decidir no broker.
    """
    try:
        async with session_scope(settings) as session:
            return await TaskRepository(session, settings).mark_failed(
                task_message.task_id, error, attempts, claim_id=claim_id
            )
    except Exception as exc:
        logger.error(
            "failed to persist task failure",
            extra={
                "task_id": str(task_message.task_id),
                "attempts": attempts,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        return None


async def _route_failure(
    message: AbstractIncomingMessage,
    task_message: TaskMessage,
    retry_count: int,
    error: str,
    settings: Settings,
    publisher: TaskPublisher,
    claim_id: uuid.UUID,
) -> None:
    """Decide entre retentar pela topologia nativa ou mandar para a DLX."""
    attempts = retry_count + 1
    log_context = {
        "task_id": str(task_message.task_id),
        "event_type": task_message.event_type,
        "retry_count": retry_count,
        "attempts": attempts,
        "error": error,
    }

    write_result = await _persist_failure(task_message, error, attempts, settings, claim_id)

    if write_result == "CLAIM_LOST":
        # Outro consumidor detem a linha AGORA e tem a propria entrega para
        # resolver a task: nackear seria uma retentativa duplicada e publicar
        # na DLX seria um dead letter espurio. Esta entrega so sai de cena.
        await message.ack()
        logger.warning("task claim lost before the failure report", extra=log_context)
        return

    if decide(retry_count, settings.task_max_retries) == "RETRY":
        # nack sem requeue: a DLX nativa encaminha para a fila de retry, que
        # cumpre o x-message-ttl e devolve a mensagem para a fila de trabalho.
        await message.nack(requeue=False)
        logger.warning("task rejected for retry", extra=log_context)
        return

    await _send_to_dead_letter(
        message,
        headers={
            X_RETRY_COUNT_HEADER: retry_count,
            X_ATTEMPTS_HEADER: attempts,
            X_FAILURE_REASON_HEADER: error,
        },
        publisher=publisher,
    )
    logger.error("task moved to dead letter queue", extra=log_context)


async def _route_concurrent_claim(
    message: AbstractIncomingMessage,
    task_message: TaskMessage,
    retry_count: int,
    settings: Settings,
    publisher: TaskPublisher,
) -> None:
    """Devolve a entrega cujo claim esta em poder de outro consumidor.

    Nao e falha da task, portanto NADA e gravado no banco. O `nack(requeue=False)`
    manda a mensagem pelo hop de retry: quando ela voltar, o outro consumidor ja
    terminou (linha COMPLETED => duplicata, ack) ou falhou (linha FAILED =>
    reclamavel). Dar ack aqui perderia a mensagem para sempre se o claim vivo
    nunca reportasse de volta.
    """
    attempts = retry_count + 1
    reason = "task claim is held by another consumer"
    log_context = {
        "task_id": str(task_message.task_id),
        "event_type": task_message.event_type,
        "retry_count": retry_count,
        "attempts": attempts,
    }

    if decide(retry_count, settings.task_max_retries) == "RETRY":
        await message.nack(requeue=False)
        logger.warning("task claim held by another consumer", extra=log_context)
        return

    await _send_to_dead_letter(
        message,
        headers={
            X_RETRY_COUNT_HEADER: retry_count,
            X_ATTEMPTS_HEADER: attempts,
            X_FAILURE_REASON_HEADER: reason,
        },
        publisher=publisher,
    )
    logger.error("task moved to dead letter queue", extra={**log_context, "error": reason})


async def on_message(
    message: AbstractIncomingMessage,
    settings: Settings,
    publisher: TaskPublisher,
    processor: Processor,
) -> None:
    """Callback de consumo com ack/nack manual."""
    try:
        task_message = parse_task_message(json.loads(message.body))
    except ValueError as exc:
        # Payload invalido nunca vai dar certo: vai direto para a dead letter queue.
        reason = f"malformed message body: {exc}"
        logger.error("malformed message sent to dead letter queue", extra={"error": reason})
        await _send_to_dead_letter(
            message,
            headers={X_FAILURE_REASON_HEADER: reason},
            publisher=publisher,
        )
        return

    retry_count = retry_count_from_headers(
        dict(message.headers) if message.headers else None,
        settings.tasks_queue_name,
    )
    attempts = retry_count + 1
    # Token de fencing desta entrega: o mesmo vale para o claim e para as duas
    # report-backs (a de sucesso dentro de `handle_task`, a de falha em
    # `_route_failure`), que estao em escopos diferentes.
    claim_id = uuid.uuid4()

    try:
        async with session_scope(settings) as session:
            outcome = await handle_task(
                task_message,
                TaskRepository(session, settings),
                attempts,
                processor=processor,
                claim_id=claim_id,
            )
    except TaskProcessingError as exc:
        await _route_failure(
            message,
            task_message,
            retry_count,
            str(exc),
            settings,
            publisher,
            claim_id,
        )
        return
    except Exception as exc:  # erro inesperado tambem segue a politica de retry/DLX
        logger.exception("unexpected error while handling task")
        await _route_failure(
            message,
            task_message,
            retry_count,
            f"{type(exc).__name__}: {exc}",
            settings,
            publisher,
            claim_id,
        )
        return

    if outcome == "SKIPPED_CONCURRENT":
        await _route_concurrent_claim(message, task_message, retry_count, settings, publisher)
        return

    await message.ack()
    logger.info(
        "message acknowledged",
        extra={
            "task_id": str(task_message.task_id),
            "outcome": outcome,
            "attempts": attempts,
        },
    )


async def run_worker(
    settings: Settings | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Consome a fila de trabalho ate `stop_event` ser setado.

    Esta e a corrotina reutilizavel: o processo do container a executa via
    `main()`, e os testes de integracao podem rodar o worker no proprio event
    loop com `asyncio.create_task(run_worker(settings=..., stop_event=...))` e
    encerrar com `stop_event.set()`.
    """
    app_settings = settings or get_settings()
    stop = stop_event or asyncio.Event()

    get_engine(app_settings)
    broker = BrokerConnection(app_settings)
    topology = await broker.connect()
    publisher = TaskPublisher.from_topology(topology, app_settings)

    # Limite de mensagens em voo por consumidor (BrokerConnection.connect() ja
    # aplica o mesmo valor; reafirmar aqui deixa a QoS do consumidor explicita).
    await broker.channel.set_qos(prefetch_count=app_settings.worker_prefetch_count)

    consumer_tag = await topology.tasks_queue.consume(
        partial(
            on_message,
            settings=app_settings,
            publisher=publisher,
            processor=partial(simulate_processing, settings=app_settings),
        ),
        no_ack=False,
    )
    logger.info(
        "worker consuming",
        extra={
            "queue": app_settings.tasks_queue_name,
            "prefetch_count": app_settings.worker_prefetch_count,
            "max_retries": app_settings.task_max_retries,
        },
    )

    try:
        await stop.wait()
    finally:
        logger.info("worker shutting down")
        await topology.tasks_queue.cancel(consumer_tag)
        await broker.close()
        await dispose_engine()
        logger.info("worker stopped")


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    """Liga SIGTERM/SIGINT ao evento de parada (shutdown gracioso)."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows nao suporta add_signal_handler: cai no handler sincrono.
            signal.signal(sig, lambda *_: stop_event.set())


async def main() -> None:
    """Processo do worker: logging, handlers de sinal e consumo."""
    settings = get_settings()
    configure_logging(settings.log_level)

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    await run_worker(settings=settings, stop_event=stop_event)


if __name__ == "__main__":
    asyncio.run(main())

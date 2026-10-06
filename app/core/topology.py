"""Topologia do RabbitMQ: especificacao pura + declaracao idempotente.

Este e o UNICO lugar do projeto onde exchanges, filas, bindings e seus argumentos
existem. A API e o worker chamam `declare_topology()` no startup, portanto os dois
processos sempre concordam sobre a topologia.

Fluxo de resiliencia desenhado aqui:

    publisher -> tasks.exchange (rk tasks.process) -> fila tasks
    nack na fila tasks -> DLX nativa -> tasks.retry.exchange -> fila tasks.retry
    fila tasks.retry expira pelo x-message-ttl -> volta para tasks.exchange
    retentativas esgotadas -> o worker publica em tasks.dlx.exchange -> fila dlx_tasks

Todos os nomes, routing keys e TTLs vem de `Settings` (app/core/config.py).
"""

import logging
from dataclasses import dataclass, field
from typing import Any

from aio_pika import ExchangeType
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue
from aio_pika.exceptions import ChannelPreconditionFailed

from app.core.config import Settings
from app.core.constants import (
    QUEUE_TYPE_CLASSIC,
    X_DEAD_LETTER_EXCHANGE_ARG,
    X_DEAD_LETTER_ROUTING_KEY_ARG,
    X_MESSAGE_TTL_ARG,
    X_QUEUE_TYPE_ARG,
)
from app.core.exceptions import TopologyError

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ExchangeSpec:
    """Exchange a ser declarada."""

    name: str
    type: ExchangeType = ExchangeType.DIRECT
    durable: bool = True


@dataclass(frozen=True, slots=True)
class QueueSpec:
    """Fila a ser declarada, com o mapa de argumentos exato."""

    name: str
    durable: bool = True
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BindingSpec:
    """Ligacao de uma fila a uma exchange por routing key."""

    queue: str
    exchange: str
    routing_key: str


@dataclass(frozen=True, slots=True)
class TopologySpec:
    """Descricao completa e inerte da topologia (nenhuma chamada ao broker)."""

    exchanges: tuple[ExchangeSpec, ...]
    queues: tuple[QueueSpec, ...]
    bindings: tuple[BindingSpec, ...]


@dataclass(frozen=True, slots=True)
class DeclaredTopology:
    """Referencias vivas devolvidas por `declare_topology()`."""

    tasks_exchange: AbstractExchange
    retry_exchange: AbstractExchange
    dlx_exchange: AbstractExchange
    tasks_queue: AbstractQueue
    retry_queue: AbstractQueue
    dlx_queue: AbstractQueue


def build_topology(settings: Settings) -> TopologySpec:
    """Monta a especificacao da topologia a partir de Settings (funcao pura)."""
    tasks_exchange = ExchangeSpec(name=settings.tasks_exchange_name)
    retry_exchange = ExchangeSpec(name=settings.retry_exchange_name)
    dlx_exchange = ExchangeSpec(name=settings.dlx_exchange_name)

    tasks_queue = QueueSpec(
        name=settings.tasks_queue_name,
        arguments={
            X_QUEUE_TYPE_ARG: QUEUE_TYPE_CLASSIC,
            # Falha na fila tasks cai na exchange de retry pela DLX nativa.
            X_DEAD_LETTER_EXCHANGE_ARG: settings.retry_exchange_name,
            X_DEAD_LETTER_ROUTING_KEY_ARG: settings.retry_routing_key,
        },
    )
    retry_queue = QueueSpec(
        name=settings.retry_queue_name,
        arguments={
            X_QUEUE_TYPE_ARG: QUEUE_TYPE_CLASSIC,
            # Depois do TTL a mensagem expira e volta para a fila de trabalho.
            X_MESSAGE_TTL_ARG: settings.retry_ttl_ms,
            X_DEAD_LETTER_EXCHANGE_ARG: settings.tasks_exchange_name,
            X_DEAD_LETTER_ROUTING_KEY_ARG: settings.tasks_routing_key,
        },
    )
    # Fila terminal: SEM x-dead-letter-exchange, senao a mensagem morta circularia.
    dlx_queue = QueueSpec(
        name=settings.dlx_queue_name,
        arguments={X_QUEUE_TYPE_ARG: QUEUE_TYPE_CLASSIC},
    )

    return TopologySpec(
        exchanges=(tasks_exchange, retry_exchange, dlx_exchange),
        queues=(tasks_queue, retry_queue, dlx_queue),
        bindings=(
            BindingSpec(
                queue=tasks_queue.name,
                exchange=tasks_exchange.name,
                routing_key=settings.tasks_routing_key,
            ),
            BindingSpec(
                queue=retry_queue.name,
                exchange=retry_exchange.name,
                routing_key=settings.retry_routing_key,
            ),
            BindingSpec(
                queue=dlx_queue.name,
                exchange=dlx_exchange.name,
                routing_key=settings.dlx_routing_key,
            ),
        ),
    )


async def declare_topology(channel: AbstractChannel, settings: Settings) -> DeclaredTopology:
    """Declara exchanges, filas e bindings; idempotente para argumentos identicos.

    Levanta `TopologyError` quando o broker responde PRECONDITION_FAILED (406):
    argumentos de fila sao imutaveis depois da criacao.
    """
    spec = build_topology(settings)
    exchanges: dict[str, AbstractExchange] = {}
    queues: dict[str, AbstractQueue] = {}

    try:
        for exchange_spec in spec.exchanges:
            exchanges[exchange_spec.name] = await channel.declare_exchange(
                exchange_spec.name,
                exchange_spec.type,
                durable=exchange_spec.durable,
            )

        for queue_spec in spec.queues:
            queues[queue_spec.name] = await channel.declare_queue(
                queue_spec.name,
                durable=queue_spec.durable,
                arguments=dict(queue_spec.arguments),
            )

        for binding in spec.bindings:
            await queues[binding.queue].bind(
                exchanges[binding.exchange],
                routing_key=binding.routing_key,
            )
    except ChannelPreconditionFailed as exc:
        raise TopologyError(
            "RabbitMQ rejected the topology declaration (PRECONDITION_FAILED / 406). "
            "Queue arguments are immutable: an existing queue cannot be redeclared with "
            "different arguments (x-message-ttl, x-dead-letter-exchange, ...). "
            "Drop the broker volume with `make reset-broker` or use a different "
            f"topology_prefix. Original error: {exc}"
        ) from exc

    logger.info(
        "topology declared",
        extra={
            "exchanges": list(exchanges),
            "queues": list(queues),
            "bindings": len(spec.bindings),
        },
    )

    return DeclaredTopology(
        tasks_exchange=exchanges[settings.tasks_exchange_name],
        retry_exchange=exchanges[settings.retry_exchange_name],
        dlx_exchange=exchanges[settings.dlx_exchange_name],
        tasks_queue=queues[settings.tasks_queue_name],
        retry_queue=queues[settings.retry_queue_name],
        dlx_queue=queues[settings.dlx_queue_name],
    )

"""Conexao com o RabbitMQ e publicacao de mensagens.

Duas responsabilidades:

* `BrokerConnection` -- ciclo de vida da conexao robusta, do canal (com publisher
  confirms e QoS) e da declaracao da topologia. Usada pela API e pelo worker.
* `TaskPublisher` -- publicacao das mensagens de task (sempre PERSISTENT) na
  exchange de trabalho e na dead letter exchange.

Nenhum nome de exchange, fila ou routing key aparece aqui: tudo vem de Settings.
"""

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import aio_pika
from aio_pika.abc import (
    AbstractExchange,
    AbstractRobustChannel,
    AbstractRobustConnection,
)

from app.core.config import Settings
from app.core.constants import (
    X_FAILED_AT_HEADER,
    X_ORIGINAL_EXCHANGE_HEADER,
    X_ORIGINAL_ROUTING_KEY_HEADER,
)
from app.core.topology import DeclaredTopology, declare_topology

logger = logging.getLogger(__name__)

CONTENT_TYPE_JSON = "application/json"

# Chaves do corpo JSON trocado entre publisher e worker.
BODY_TASK_ID = "task_id"
BODY_EVENT_TYPE = "event_type"
BODY_PAYLOAD = "payload"


class BrokerConnection:
    """Conexao robusta + canal unico + topologia declarada."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._topology: DeclaredTopology | None = None

    @property
    def connection(self) -> AbstractRobustConnection:
        if self._connection is None:
            raise RuntimeError("broker is not connected; call connect() first")
        return self._connection

    @property
    def channel(self) -> AbstractRobustChannel:
        if self._channel is None:
            raise RuntimeError("broker is not connected; call connect() first")
        return self._channel

    @property
    def topology(self) -> DeclaredTopology:
        if self._topology is None:
            raise RuntimeError("topology was not declared; call connect() first")
        return self._topology

    async def connect(self) -> DeclaredTopology:
        """Abre a conexao, o canal com publisher confirms e declara a topologia."""
        self._connection = await aio_pika.connect_robust(self._settings.amqp_url)
        self._channel = await self._connection.channel(publisher_confirms=True)
        await self._channel.set_qos(prefetch_count=self._settings.worker_prefetch_count)
        self._topology = await declare_topology(self._channel, self._settings)
        logger.info(
            "broker connected",
            extra={"prefetch_count": self._settings.worker_prefetch_count},
        )
        return self._topology

    async def close(self) -> None:
        """Fecha canal e conexao (tolerante a chamadas repetidas)."""
        if self._channel is not None and not self._channel.is_closed:
            await self._channel.close()
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
        self._channel = None
        self._connection = None
        self._topology = None
        logger.info("broker disconnected")

    def healthcheck(self) -> bool:
        """True quando existe uma conexao e um canal abertos."""
        return (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
        )


class TaskPublisher:
    """Publica as mensagens de task de forma persistente."""

    def __init__(
        self,
        tasks_exchange: AbstractExchange,
        dlx_exchange: AbstractExchange,
        settings: Settings,
    ) -> None:
        self._tasks_exchange = tasks_exchange
        self._dlx_exchange = dlx_exchange
        self._settings = settings

    @classmethod
    def from_topology(cls, topology: DeclaredTopology, settings: Settings) -> "TaskPublisher":
        """Atalho para montar o publisher a partir da topologia declarada."""
        return cls(
            tasks_exchange=topology.tasks_exchange,
            dlx_exchange=topology.dlx_exchange,
            settings=settings,
        )

    async def publish(
        self,
        task_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        """Publica o evento na exchange de trabalho e espera o confirm do broker."""
        body = json.dumps(
            {
                BODY_TASK_ID: str(task_id),
                BODY_EVENT_TYPE: event_type,
                BODY_PAYLOAD: payload,
            }
        ).encode()

        message = aio_pika.Message(
            body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type=CONTENT_TYPE_JSON,
            message_id=str(task_id),
        )
        await self._tasks_exchange.publish(
            message,
            routing_key=self._settings.tasks_routing_key,
        )
        logger.info(
            "task published",
            extra={"task_id": str(task_id), "event_type": event_type},
        )

    async def publish_to_dlx(
        self,
        body: bytes,
        headers: dict[str, Any],
        original_exchange: str,
        original_routing_key: str,
    ) -> None:
        """Publica o corpo original na dead letter exchange, preservando a origem."""
        dlx_headers: dict[str, Any] = {
            **headers,
            X_ORIGINAL_EXCHANGE_HEADER: original_exchange,
            X_ORIGINAL_ROUTING_KEY_HEADER: original_routing_key,
        }
        dlx_headers.setdefault(X_FAILED_AT_HEADER, datetime.now(UTC).isoformat())

        message = aio_pika.Message(
            body,
            headers=dlx_headers,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type=CONTENT_TYPE_JSON,
        )
        await self._dlx_exchange.publish(
            message,
            routing_key=self._settings.dlx_routing_key,
        )
        logger.warning(
            "task sent to dead letter queue",
            extra={
                "queue": self._settings.dlx_queue_name,
                "original_routing_key": original_routing_key,
            },
        )

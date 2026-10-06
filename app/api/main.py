"""Fabrica da aplicacao FastAPI (publisher).

O lifespan conecta no broker, declara a topologia (a mesma funcao usada pelo
worker) e deixa o publisher em `app.state` para as dependencias da API.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1 import health, tasks
from app.core.broker import BrokerConnection, TaskPublisher
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.db.session import dispose_engine, get_engine

logger = logging.getLogger(__name__)

API_V1_PREFIX = "/api/v1"


def create_app(settings: Settings | None = None) -> FastAPI:
    """Monta a aplicacao. Aceita Settings injetado para facilitar os testes."""
    app_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(app_settings.log_level)
        app.state.settings = app_settings

        get_engine(app_settings)

        broker = BrokerConnection(app_settings)
        topology = await broker.connect()
        app.state.broker = broker
        app.state.publisher = TaskPublisher.from_topology(topology, app_settings)
        logger.info("api started", extra={"environment": app_settings.environment})

        try:
            yield
        finally:
            app.state.publisher = None
            await broker.close()
            await dispose_engine()
            logger.info("api stopped")

    app = FastAPI(
        title=app_settings.app_name,
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(tasks.router, prefix=API_V1_PREFIX)
    app.include_router(health.router)
    return app


app = create_app()

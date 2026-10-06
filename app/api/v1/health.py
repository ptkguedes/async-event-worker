"""Healthcheck da API: reporta banco e broker separadamente."""

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status

from app.api.deps import get_broker, get_settings_dep
from app.api.schemas import HealthResponse
from app.core.broker import BrokerConnection
from app.core.config import Settings
from app.db.session import ping

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])

STATUS_OK = "ok"
STATUS_ERROR = "error"
STATUS_DEGRADED = "degraded"


async def _database_status(settings: Settings) -> str:
    try:
        await ping(settings)
    except Exception as exc:  # pragma: no cover - depende do banco real
        logger.warning("database healthcheck failed", extra={"error": str(exc)})
        return STATUS_ERROR
    return STATUS_OK


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Estado da API e das suas dependencias",
)
async def health(
    response: Response,
    settings: Annotated[Settings, Depends(get_settings_dep)],
    broker: Annotated[BrokerConnection | None, Depends(get_broker)],
) -> HealthResponse:
    """200 quando banco e broker respondem; 503 identificando o degradado."""
    database = await _database_status(settings)
    broker_status = STATUS_OK if broker is not None and broker.healthcheck() else STATUS_ERROR

    healthy = database == STATUS_OK and broker_status == STATUS_OK
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        status=STATUS_OK if healthy else STATUS_DEGRADED,
        database=database,
        broker=broker_status,
    )

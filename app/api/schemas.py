"""Contratos HTTP da API (Pydantic v2)."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.db.models import TaskStatus


class TaskCreateRequest(BaseModel):
    """Corpo do POST /api/v1/tasks.

    `task_id` e opcional: quando o cliente informa um, ele e honrado e passa a ser
    a chave de idempotencia do evento; quando falta, a API gera um uuid4.
    """

    event_type: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any]
    task_id: UUID | None = None


class TaskAcceptedResponse(BaseModel):
    """Resposta 202: a task foi enfileirada, nada foi processado ainda."""

    task_id: UUID
    status: str = TaskStatus.PENDING.value
    accepted_at: datetime


class TaskResponse(BaseModel):
    """Espelho do modelo Task persistido."""

    model_config = ConfigDict(from_attributes=True)

    task_id: UUID
    event_type: str
    payload: dict[str, Any]
    status: TaskStatus
    attempts: int
    result: dict[str, Any] | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime


class HealthResponse(BaseModel):
    """Resposta do GET /health, com o estado de cada dependencia."""

    status: str
    database: str
    broker: str

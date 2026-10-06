"""Endpoints de task: publicacao assincrona e consulta do resultado."""

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_publisher, get_repository
from app.api.schemas import TaskAcceptedResponse, TaskCreateRequest, TaskResponse
from app.core.broker import TaskPublisher
from app.db.repository import TaskRepositoryProtocol

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=TaskAcceptedResponse,
    summary="Aceita um evento e publica na fila",
)
async def create_task(
    body: TaskCreateRequest,
    publisher: Annotated[TaskPublisher, Depends(get_publisher)],
) -> TaskAcceptedResponse:
    """Publica o evento e responde 202 imediatamente.

    Nenhuma regra de negocio roda aqui: o processamento e do worker. A resposta
    nao espera o consumo da mensagem.
    """
    task_id = body.task_id or uuid4()
    await publisher.publish(
        task_id=task_id,
        event_type=body.event_type,
        payload=body.payload,
    )
    return TaskAcceptedResponse(task_id=task_id, accepted_at=datetime.now(UTC))


@router.get(
    "/{task_id}",
    response_model=TaskResponse,
    summary="Consulta o resultado persistido de uma task",
)
async def get_task(
    task_id: UUID,
    repository: Annotated[TaskRepositoryProtocol, Depends(get_repository)],
) -> TaskResponse:
    """Devolve a task persistida ou 404 enquanto o worker nao a tiver gravado."""
    task = await repository.get(task_id)
    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="task not found",
        )
    return TaskResponse.model_validate(task)

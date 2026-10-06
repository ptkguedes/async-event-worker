"""A API responde 202 sem processar nada: ela so publica e devolve o task_id."""

import uuid
from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.deps import get_publisher, get_repository
from app.api.main import create_app
from app.core.config import Settings
from app.db.models import TaskStatus
from tests.fakes import FakePublisher, FakeTaskRepository, make_task


@pytest.fixture
def app(
    settings: Settings,
    fake_publisher: FakePublisher,
    fake_repository: FakeTaskRepository,
) -> FastAPI:
    """App real com broker e banco substituidos pelos fakes."""
    application = create_app(settings)
    application.dependency_overrides[get_publisher] = lambda: fake_publisher
    application.dependency_overrides[get_repository] = lambda: fake_repository
    return application


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """Cliente HTTP em processo (o lifespan nao roda: nada externo e aberto)."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as http_client:
        yield http_client


async def test_post_task_returns_202_with_task_id(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/tasks",
        json={"event_type": "demo", "payload": {}},
    )

    assert response.status_code == 202
    body = response.json()
    assert uuid.UUID(body["task_id"])
    assert body["status"] == TaskStatus.PENDING.value
    assert body["accepted_at"]


async def test_post_task_publishes_exactly_one_persistent_message(
    client: AsyncClient,
    fake_publisher: FakePublisher,
    settings: Settings,
) -> None:
    response = await client.post(
        "/api/v1/tasks",
        json={"event_type": "demo", "payload": {"amount": 10}},
    )

    assert len(fake_publisher.messages) == 1
    message = fake_publisher.messages[0]
    assert message.exchange == settings.tasks_exchange_name
    assert message.routing_key == settings.tasks_routing_key == "tasks.process"
    # 2 == DeliveryMode.PERSISTENT: a mensagem sobrevive a um restart do broker.
    assert message.delivery_mode == 2
    assert message.decoded == {
        "task_id": response.json()["task_id"],
        "event_type": "demo",
        "payload": {"amount": 10},
    }


async def test_client_provided_task_id_is_honored(
    client: AsyncClient,
    fake_publisher: FakePublisher,
) -> None:
    task_id = uuid.uuid4()

    response = await client.post(
        "/api/v1/tasks",
        json={"event_type": "demo", "payload": {}, "task_id": str(task_id)},
    )

    assert response.status_code == 202
    assert response.json()["task_id"] == str(task_id)
    message = fake_publisher.messages[0]
    assert message.decoded["task_id"] == str(task_id)
    assert message.message_id == str(task_id)


async def test_post_task_with_invalid_body_is_rejected_without_publishing(
    client: AsyncClient,
    fake_publisher: FakePublisher,
) -> None:
    response = await client.post("/api/v1/tasks", json={"event_type": "", "payload": {}})

    assert response.status_code == 422
    assert fake_publisher.messages == []


async def test_get_unknown_task_returns_404(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/tasks/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_get_existing_task_returns_the_persisted_row(
    client: AsyncClient,
    fake_repository: FakeTaskRepository,
) -> None:
    task = fake_repository.seed(
        make_task(
            task_id=uuid.uuid4(),
            event_type="demo",
            payload={"a": 1},
            status=TaskStatus.COMPLETED,
            attempts=1,
            result={"ok": True},
        )
    )

    response = await client.get(f"/api/v1/tasks/{task.task_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["task_id"] == str(task.task_id)
    assert body["status"] == TaskStatus.COMPLETED.value
    assert body["result"] == {"ok": True}
    assert body["attempts"] == 1

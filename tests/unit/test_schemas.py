"""Validacao dos contratos HTTP: o que a API aceita e o que ela recusa."""

import uuid

import pytest
from pydantic import ValidationError

from app.api.schemas import TaskCreateRequest, TaskResponse
from app.db.models import TaskStatus
from tests.fakes import make_task


def test_empty_event_type_is_rejected() -> None:
    with pytest.raises(ValidationError):
        TaskCreateRequest(event_type="", payload={})


def test_event_type_longer_than_100_chars_is_rejected() -> None:
    with pytest.raises(ValidationError):
        TaskCreateRequest(event_type="x" * 101, payload={})


def test_event_type_with_100_chars_is_accepted() -> None:
    request = TaskCreateRequest(event_type="x" * 100, payload={})
    assert len(request.event_type) == 100


def test_invalid_task_id_is_rejected() -> None:
    with pytest.raises(ValidationError):
        TaskCreateRequest(event_type="demo", payload={}, task_id="not-a-uuid")


def test_missing_task_id_stays_none() -> None:
    request = TaskCreateRequest(event_type="demo", payload={})
    assert request.task_id is None


def test_provided_task_id_is_parsed_as_uuid() -> None:
    task_id = uuid.uuid4()
    request = TaskCreateRequest(event_type="demo", payload={}, task_id=str(task_id))
    assert request.task_id == task_id


def test_payload_accepts_nested_dict() -> None:
    payload = {"order": {"id": 7, "items": [{"sku": "a"}, {"sku": "b"}]}, "flag": True}
    request = TaskCreateRequest(event_type="demo", payload=payload)
    assert request.payload == payload


def test_missing_payload_is_rejected() -> None:
    with pytest.raises(ValidationError):
        TaskCreateRequest(event_type="demo")


def test_task_response_reads_from_the_orm_model() -> None:
    task = make_task(
        task_id=uuid.uuid4(),
        event_type="demo",
        payload={"a": 1},
        status=TaskStatus.COMPLETED,
        attempts=2,
        result={"ok": True},
    )
    response = TaskResponse.model_validate(task)
    assert response.task_id == task.task_id
    assert response.status is TaskStatus.COMPLETED
    assert response.attempts == 2
    assert response.result == {"ok": True}
    assert response.error is None

"""Trava a politica de retentativa: leitura do x-death e decisao de destino."""

from typing import Any

import pytest

from app.worker.retry import RetryDecision, decide, retry_count_from_headers

TASKS_QUEUE = "tasks"
RETRY_QUEUE = "tasks.retry"


def _death(queue: str, reason: str, count: int) -> dict[str, Any]:
    """Entrada de x-death como o RabbitMQ a entrega."""
    return {"queue": queue, "reason": reason, "count": count, "exchange": "tasks.exchange"}


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        pytest.param(None, 0, id="headers-ausentes"),
        pytest.param({}, 0, id="headers-vazios"),
        pytest.param({"x-death": []}, 0, id="x-death-vazio"),
        pytest.param(
            {"x-death": [_death(RETRY_QUEUE, "expired", 1)]},
            0,
            id="so-expired-na-fila-de-retry",
        ),
        pytest.param(
            {"x-death": [_death(TASKS_QUEUE, "rejected", 2)]},
            2,
            id="rejected-com-count-2",
        ),
        pytest.param(
            {
                "x-death": [
                    _death(TASKS_QUEUE, "rejected", 3),
                    _death(RETRY_QUEUE, "expired", 3),
                ]
            },
            3,
            id="rejected-e-expired-so-rejected-conta",
        ),
        pytest.param(
            {"x-death": [_death("outra.fila", "rejected", 5)]},
            0,
            id="entrada-de-outra-fila-e-ignorada",
        ),
        pytest.param({"x-death": "nao-e-lista"}, 0, id="x-death-malformado"),
    ],
)
def test_retry_count_from_headers(headers: dict[str, Any] | None, expected: int) -> None:
    assert retry_count_from_headers(headers, TASKS_QUEUE) == expected


def test_retry_count_uses_the_configured_queue_name() -> None:
    headers = {"x-death": [_death("test_tasks", "rejected", 1)]}
    assert retry_count_from_headers(headers, "test_tasks") == 1
    assert retry_count_from_headers(headers, TASKS_QUEUE) == 0


@pytest.mark.parametrize(
    ("retry_count", "expected"),
    [(0, "RETRY"), (1, "RETRY"), (2, "RETRY"), (3, "DEAD_LETTER"), (4, "DEAD_LETTER")],
)
def test_decide_with_three_max_retries(retry_count: int, expected: RetryDecision) -> None:
    assert decide(retry_count, max_retries=3) == expected


def test_three_max_retries_means_four_deliveries() -> None:
    """1 entrega original + 3 retentativas antes da dead letter queue."""
    deliveries = 0
    retry_count = 0
    while decide(retry_count, max_retries=3) == "RETRY":
        deliveries += 1
        retry_count += 1
    deliveries += 1  # a entrega que resulta em DEAD_LETTER
    assert deliveries == 4

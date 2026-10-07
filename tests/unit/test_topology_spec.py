"""Trava a topologia: os mapas de argumentos sao o contrato do fluxo de retry/DLX."""

from aio_pika import ExchangeType

from app.core.config import Settings
from app.core.topology import build_topology


def _settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)


def _queues_by_name(settings: Settings) -> dict[str, dict[str, object]]:
    return {queue.name: queue.arguments for queue in build_topology(settings).queues}


def test_topology_has_three_exchanges_three_queues_three_bindings() -> None:
    spec = build_topology(_settings())
    assert len(spec.exchanges) == 3
    assert len(spec.queues) == 3
    assert len(spec.bindings) == 3


def test_every_exchange_is_direct_and_durable() -> None:
    spec = build_topology(_settings())
    assert [exchange.type for exchange in spec.exchanges] == [ExchangeType.DIRECT] * 3
    assert all(exchange.durable for exchange in spec.exchanges)
    assert [exchange.name for exchange in spec.exchanges] == [
        "tasks.exchange",
        "tasks.retry.exchange",
        "tasks.dlx.exchange",
    ]


def test_every_queue_is_durable_and_classic() -> None:
    spec = build_topology(_settings())
    assert all(queue.durable for queue in spec.queues)
    assert all(queue.arguments["x-queue-type"] == "classic" for queue in spec.queues)


def test_tasks_queue_dead_letters_to_the_retry_exchange() -> None:
    arguments = _queues_by_name(_settings())["tasks"]
    assert arguments == {
        "x-queue-type": "classic",
        "x-dead-letter-exchange": "tasks.retry.exchange",
        "x-dead-letter-routing-key": "tasks.retry",
    }
    assert "x-message-ttl" not in arguments


def test_retry_queue_waits_the_ttl_and_returns_to_the_tasks_exchange() -> None:
    arguments = _queues_by_name(_settings())["tasks.retry"]
    assert arguments == {
        "x-queue-type": "classic",
        "x-message-ttl": 10_000,
        "x-dead-letter-exchange": "tasks.exchange",
        "x-dead-letter-routing-key": "tasks.process",
    }


def test_retry_ttl_comes_from_settings() -> None:
    arguments = _queues_by_name(_settings(retry_ttl_ms=777))["tasks.retry"]
    assert arguments["x-message-ttl"] == 777


def test_dlx_queue_is_terminal() -> None:
    arguments = _queues_by_name(_settings())["dlx_tasks"]
    assert arguments == {"x-queue-type": "classic"}
    assert "x-dead-letter-exchange" not in arguments
    assert "x-dead-letter-routing-key" not in arguments


def test_bindings_use_the_configured_routing_keys() -> None:
    bindings = {
        binding.queue: (binding.exchange, binding.routing_key)
        for binding in build_topology(_settings()).bindings
    }
    assert bindings == {
        "tasks": ("tasks.exchange", "tasks.process"),
        "tasks.retry": ("tasks.retry.exchange", "tasks.retry"),
        "dlx_tasks": ("tasks.dlx.exchange", "tasks.dead"),
    }


def test_topology_prefix_is_applied_to_all_six_names() -> None:
    spec = build_topology(_settings(topology_prefix="test_"))
    names = [exchange.name for exchange in spec.exchanges] + [queue.name for queue in spec.queues]
    assert names == [
        "test_tasks.exchange",
        "test_tasks.retry.exchange",
        "test_tasks.dlx.exchange",
        "test_tasks",
        "test_tasks.retry",
        "test_dlx_tasks",
    ]
    assert all(binding.queue.startswith("test_") for binding in spec.bindings)
    assert all(binding.exchange.startswith("test_") for binding in spec.bindings)


def test_prefixed_queues_dead_letter_to_prefixed_exchanges() -> None:
    queues = _queues_by_name(_settings(topology_prefix="test_"))
    assert queues["test_tasks"]["x-dead-letter-exchange"] == "test_tasks.retry.exchange"
    assert queues["test_tasks.retry"]["x-dead-letter-exchange"] == "test_tasks.exchange"

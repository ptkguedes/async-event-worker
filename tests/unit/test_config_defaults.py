"""Trava os defaults de topologia e resiliencia: sao contrato, nao detalhe."""

from app.core.config import Settings, get_settings


def _settings(**overrides: object) -> Settings:
    # _env_file=None isola o teste de um .env presente na maquina do dev.
    return Settings(_env_file=None, **overrides)


def test_exchange_defaults() -> None:
    settings = _settings()
    assert settings.tasks_exchange == "tasks.exchange"
    assert settings.retry_exchange == "tasks.retry.exchange"
    assert settings.dlx_exchange == "tasks.dlx.exchange"


def test_queue_defaults() -> None:
    settings = _settings()
    assert settings.tasks_queue == "tasks"
    assert settings.retry_queue == "tasks.retry"
    assert settings.dlx_queue == "dlx_tasks"


def test_routing_key_defaults() -> None:
    settings = _settings()
    assert settings.tasks_routing_key == "tasks.process"
    assert settings.retry_routing_key == "tasks.retry"
    assert settings.dlx_routing_key == "tasks.dead"


def test_resilience_defaults() -> None:
    settings = _settings()
    assert settings.task_max_retries == 3
    assert settings.retry_ttl_ms == 5000


def test_empty_prefix_keeps_names_unchanged() -> None:
    settings = _settings(topology_prefix="")
    assert settings.tasks_queue_name == "tasks"
    assert settings.retry_queue_name == "tasks.retry"
    assert settings.dlx_queue_name == "dlx_tasks"
    assert settings.tasks_exchange_name == "tasks.exchange"
    assert settings.retry_exchange_name == "tasks.retry.exchange"
    assert settings.dlx_exchange_name == "tasks.dlx.exchange"


def test_prefix_is_applied_to_every_name() -> None:
    settings = _settings(topology_prefix="test_")
    assert settings.tasks_queue_name == "test_tasks"
    assert settings.retry_queue_name == "test_tasks.retry"
    assert settings.dlx_queue_name == "test_dlx_tasks"
    assert settings.tasks_exchange_name == "test_tasks.exchange"
    assert settings.retry_exchange_name == "test_tasks.retry.exchange"
    assert settings.dlx_exchange_name == "test_tasks.dlx.exchange"


def test_get_settings_is_cached() -> None:
    assert get_settings() is get_settings()

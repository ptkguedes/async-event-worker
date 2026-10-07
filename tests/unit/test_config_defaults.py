"""Trava os defaults de topologia e resiliencia: sao contrato, nao detalhe.

A segunda metade do arquivo cobre a derivacao das URLs: as variaveis raiz
(`POSTGRES_*`, `RABBITMQ_*`) sao a fonte unica e a URL completa continua sendo
um override valido.
"""

import pytest

from app.core.config import Settings, get_settings

DEFAULT_DATABASE_URL = "postgresql+asyncpg://app:app@postgres:5432/async_event_worker"
DEFAULT_AMQP_URL = "amqp://guest:guest@rabbitmq:5672/"

# Variaveis que, exportadas na maquina do dev, falsificariam os testes de default.
_URL_ENV_VARS = (
    "DATABASE_URL",
    "AMQP_URL",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "RABBITMQ_DEFAULT_USER",
    "RABBITMQ_DEFAULT_PASS",
    "RABBITMQ_HOST",
    "RABBITMQ_PORT",
    "RABBITMQ_VHOST",
)


def _settings(**overrides: object) -> Settings:
    # _env_file=None isola o teste de um .env presente na maquina do dev.
    return Settings(_env_file=None, **overrides)


@pytest.fixture
def clean_url_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove do ambiente tudo que participa da derivacao das URLs."""
    for name in _URL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


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
    assert settings.retry_ttl_ms == 10_000


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


def test_claim_lease_default_sits_above_the_db_timeouts_and_below_the_retry_budget() -> None:
    settings = _settings()
    assert settings.claim_lease_seconds == 15.0

    # PISO: a lease cobre o pior caso de I/O de banco de UM worker. Se ela
    # fosse menor que a soma dos timeouts, um worker legitimamente lento
    # perderia o claim ainda processando e outro consumidor rodaria o efeito
    # colateral de novo -- o fencing protege o banco, nao o efeito colateral.
    db_io_ceiling_seconds = (
        settings.db_connect_timeout_seconds + settings.db_statement_timeout_ms / 1000
    )
    assert settings.claim_lease_seconds > db_io_ceiling_seconds

    # E obviamente acima do proprio processamento simulado.
    assert settings.claim_lease_seconds >= settings.processing_delay_seconds

    # TETO: um claim deixado por um worker morto tem de expirar com folga
    # dentro do orcamento de retentativas. Com entregas em ~0s/10s/20s/30s, a
    # lease precisa cair antes da 3a para a 4a sobrar de reserva.
    assert settings.claim_lease_seconds < 2 * settings.retry_ttl_ms / 1000


def test_claim_lease_must_be_positive() -> None:
    with pytest.raises(ValueError):
        _settings(claim_lease_seconds=0)


def test_database_timeout_defaults() -> None:
    settings = _settings()
    assert settings.db_connect_timeout_seconds == 3.0
    assert settings.db_statement_timeout_ms == 5_000
    # Folga sobre o maior statement da app (single-row pela PRIMARY KEY, ordem
    # de milissegundos), mas ABAIXO do lease do claim: a soma dos dois timeouts
    # e o teto do tempo que um worker pode ficar preso em I/O, e esse teto tem
    # de caber dentro da lease. A ordenacao inversa (statement > lease) deixava
    # uma janela em que o claim expirava com o worker ainda processando.
    db_io_ceiling_seconds = (
        settings.db_connect_timeout_seconds + settings.db_statement_timeout_ms / 1000
    )
    assert db_io_ceiling_seconds < settings.claim_lease_seconds


def test_database_timeouts_must_be_positive() -> None:
    with pytest.raises(ValueError):
        _settings(db_connect_timeout_seconds=0)
    with pytest.raises(ValueError):
        _settings(db_statement_timeout_ms=-1)


def test_derived_urls_match_the_previous_literal_defaults(clean_url_env: None) -> None:
    """A derivacao default e byte a byte igual as URLs que eram literais."""
    settings = _settings()
    assert settings.database_url == DEFAULT_DATABASE_URL
    assert settings.amqp_url == DEFAULT_AMQP_URL


def test_explicit_urls_win_over_derivation(clean_url_env: None) -> None:
    settings = _settings(
        database_url="postgresql+asyncpg://u:p@db.example:6543/other",
        amqp_url="amqp://u:p@mq.example:5673/vh",
        postgres_user="ignored",
        rabbitmq_user="ignored",
    )
    assert settings.database_url == "postgresql+asyncpg://u:p@db.example:6543/other"
    assert settings.amqp_url == "amqp://u:p@mq.example:5673/vh"


def test_explicit_urls_from_the_environment_win(
    clean_url_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://env:env@envhost:5432/envdb")
    monkeypatch.setenv("AMQP_URL", "amqp://env:env@envmq:5672/")
    settings = _settings()
    assert settings.database_url == "postgresql+asyncpg://env:env@envhost:5432/envdb"
    assert settings.amqp_url == "amqp://env:env@envmq:5672/"


def test_root_credentials_reach_the_derived_urls(
    clean_url_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sobrescrever a senha em UM lugar basta: o objetivo da unificacao."""
    monkeypatch.setenv("POSTGRES_USER", "svc")
    monkeypatch.setenv("POSTGRES_PASSWORD", "s3cr3t")
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    monkeypatch.setenv("POSTGRES_DB", "events")
    monkeypatch.setenv("RABBITMQ_DEFAULT_USER", "mq")
    monkeypatch.setenv("RABBITMQ_DEFAULT_PASS", "mqpass")
    monkeypatch.setenv("RABBITMQ_HOST", "broker")
    monkeypatch.setenv("RABBITMQ_PORT", "5673")

    settings = _settings()

    assert settings.database_url == "postgresql+asyncpg://svc:s3cr3t@db:6543/events"
    assert settings.amqp_url == "amqp://mq:mqpass@broker:5673/"


def test_derived_urls_percent_encode_the_password(clean_url_env: None) -> None:
    settings = _settings(postgres_password="p@ss:w/ord", rabbitmq_password="p@ss:w/ord")
    assert "p%40ss%3Aw%2Ford" in settings.database_url
    assert "p%40ss%3Aw%2Ford" in settings.amqp_url
    # O separador de credencial/host nao foi corrompido pela senha.
    assert settings.database_url.endswith("@postgres:5432/async_event_worker")
    assert settings.amqp_url.endswith("@rabbitmq:5672/")


def test_named_vhost_becomes_a_url_path(clean_url_env: None) -> None:
    assert (
        _settings(rabbitmq_vhost="app_vhost").amqp_url
        == "amqp://guest:guest@rabbitmq:5672/app_vhost"
    )
    assert _settings(rabbitmq_vhost="/").amqp_url == "amqp://guest:guest@rabbitmq:5672/"
    assert _settings(rabbitmq_vhost="").amqp_url == "amqp://guest:guest@rabbitmq:5672/"

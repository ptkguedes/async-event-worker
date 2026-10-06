"""Fixtures compartilhadas da suite.

A fixture `settings` isola os testes: prefixo proprio de topologia, TTL curto e
processamento sem delay.
"""

import pytest

from app.core.config import Settings
from tests.fakes import FakePublisher, FakeTaskRepository

TEST_TOPOLOGY_PREFIX = "test_"


@pytest.fixture
def settings() -> Settings:
    """Settings de teste (_env_file=None ignora um .env presente na maquina)."""
    return Settings(
        _env_file=None,
        topology_prefix=TEST_TOPOLOGY_PREFIX,
        retry_ttl_ms=500,
        processing_delay_seconds=0,
    )


@pytest.fixture
def fake_repository() -> FakeTaskRepository:
    """Repositorio em memoria."""
    return FakeTaskRepository()


@pytest.fixture
def fake_publisher(settings: Settings) -> FakePublisher:
    """Publisher que apenas acumula as mensagens."""
    return FakePublisher(settings)

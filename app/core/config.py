"""Configuracao unica da aplicacao.

Toda URL, nome de fila/exchange, routing key, limite de retry e TTL vive aqui.
Nenhum literal de nome, TTL ou limite deve aparecer fora deste modulo.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Valores de configuracao lidos do ambiente (ou do arquivo .env)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Aplicacao
    app_name: str = "async-event-worker"
    environment: str = "local"
    log_level: str = "INFO"

    # API HTTP
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Banco de dados
    database_url: str = "postgresql+asyncpg://app:app@postgres:5432/async_event_worker"
    db_echo: bool = False
    db_pool_size: int = 5
    db_max_overflow: int = 10

    # Broker
    amqp_url: str = "amqp://guest:guest@rabbitmq:5672/"

    # Topologia: o prefixo permite isolar a suite de integracao do worker do Compose
    topology_prefix: str = ""
    tasks_exchange: str = "tasks.exchange"
    retry_exchange: str = "tasks.retry.exchange"
    dlx_exchange: str = "tasks.dlx.exchange"
    tasks_queue: str = "tasks"
    retry_queue: str = "tasks.retry"
    dlx_queue: str = "dlx_tasks"
    tasks_routing_key: str = "tasks.process"
    retry_routing_key: str = "tasks.retry"
    dlx_routing_key: str = "tasks.dead"

    # Resiliencia: 3 retentativas automaticas (4 processamentos no total) antes da DLX
    task_max_retries: int = 3
    retry_ttl_ms: int = 5000

    # Worker
    worker_prefetch_count: int = 10
    processing_delay_seconds: float = 0.5

    def _prefixed(self, name: str) -> str:
        return f"{self.topology_prefix}{name}"

    @property
    def tasks_queue_name(self) -> str:
        return self._prefixed(self.tasks_queue)

    @property
    def retry_queue_name(self) -> str:
        return self._prefixed(self.retry_queue)

    @property
    def dlx_queue_name(self) -> str:
        return self._prefixed(self.dlx_queue)

    @property
    def tasks_exchange_name(self) -> str:
        return self._prefixed(self.tasks_exchange)

    @property
    def retry_exchange_name(self) -> str:
        return self._prefixed(self.retry_exchange)

    @property
    def dlx_exchange_name(self) -> str:
        return self._prefixed(self.dlx_exchange)


@lru_cache
def get_settings() -> Settings:
    """Instancia unica de Settings (cacheada para o processo todo)."""
    return Settings()

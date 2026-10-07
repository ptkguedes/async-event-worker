"""Configuracao unica da aplicacao.

Toda URL, nome de fila/exchange, routing key, limite de retry e TTL vive aqui.
Nenhum literal de nome, TTL ou limite deve aparecer fora deste modulo.

DERIVACAO UNICA DAS URLS: `database_url` e `amqp_url` nao sao mais montadas em
varios lugares (Compose, .env, README). Elas sao derivadas das variaveis raiz de
credencial e host (`POSTGRES_*`, `RABBITMQ_*`) por `_derive_urls`, de modo que
sobrescrever uma senha em UM lugar basta. Uma URL completa informada pelo
ambiente continua vencendo (override explicito).
"""

from functools import lru_cache
from urllib.parse import quote

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Valores de configuracao lidos do ambiente (ou do arquivo .env)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    # Aplicacao
    app_name: str = "async-event-worker"
    environment: str = "local"
    log_level: str = "INFO"

    # API HTTP
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Banco de dados: as variaveis raiz abaixo sao a fonte unica da verdade.
    # Sao exatamente as mesmas que o docker-compose.yml entrega ao container do
    # Postgres, por isso trocar a senha num lugar vale para a app tambem.
    postgres_user: str = "app"
    postgres_password: str = "app"
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "async_event_worker"
    # Vazio => derivada das variaveis acima. Preenchida => override explicito.
    database_url: str = ""
    db_echo: bool = False
    db_pool_size: int = 5
    db_max_overflow: int = 10

    # Broker: idem, os aliases sao os nomes que a imagem do RabbitMQ ja usa.
    rabbitmq_user: str = Field("guest", validation_alias="RABBITMQ_DEFAULT_USER")
    rabbitmq_password: str = Field("guest", validation_alias="RABBITMQ_DEFAULT_PASS")
    rabbitmq_host: str = "rabbitmq"
    rabbitmq_port: int = 5672
    rabbitmq_vhost: str = "/"
    # Vazio => derivada das variaveis acima. Preenchida => override explicito.
    amqp_url: str = ""

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
    # Duracao do lease do claim idempotente. Regra: lease >= duracao maxima do
    # processamento (senao um claim vivo seria considerado obsoleto) e
    # lease <= retry_ttl_ms (senao um claim deixado por um worker morto so
    # voltaria a ser reclamavel depois do orcamento de retentativas acabar,
    # mandando para a DLX uma task que nunca falhou).
    claim_lease_seconds: float = Field(5.0, gt=0)

    @model_validator(mode="after")
    def _derive_urls(self) -> "Settings":
        """Monta `database_url`/`amqp_url` a partir das variaveis raiz.

        Um valor nao vazio vence e nada e derivado -- e isso que mantem
        `TEST_DATABASE_URL`/`TEST_AMQP_URL` e os overrides do README
        funcionando. Usuario, senha e vhost sao percent-encoded para que um
        `@`, `:` ou `/` na senha nao corrompa a URL.
        """
        if not self.database_url.strip():
            self.database_url = (
                f"postgresql+asyncpg://{quote(self.postgres_user, safe='')}"
                f":{quote(self.postgres_password, safe='')}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            )

        if not self.amqp_url.strip():
            # vhost "/" (o default do RabbitMQ) ou vazio => path vazio, isto e a
            # URL termina em "/"; um vhost nomeado vira "/<nome>".
            vhost = quote(self.rabbitmq_vhost.strip("/"), safe="")
            self.amqp_url = (
                f"amqp://{quote(self.rabbitmq_user, safe='')}"
                f":{quote(self.rabbitmq_password, safe='')}"
                f"@{self.rabbitmq_host}:{self.rabbitmq_port}/{vhost}"
            )

        return self

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

"""connect_args da engine: onde cada timeout vai e em que unidade.

O risco real aqui nao e o valor, e o LUGAR: `timeout` e parametro do
`asyncpg.connect` (estabelecimento da conexao), enquanto `statement_timeout` e
um GUC do Postgres e so vigora dentro de `server_settings`, em milissegundos e
como string. Trocar os dois de lugar nao daria erro visivel -- o timeout
simplesmente nao existiria.
"""

from app.core.config import Settings
from app.db.session import build_connect_args


def _settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)


def test_connect_args_has_exactly_the_three_asyncpg_keys() -> None:
    assert set(build_connect_args(_settings())) == {
        "timeout",
        "command_timeout",
        "server_settings",
    }


def test_connect_timeout_is_the_asyncpg_connect_parameter() -> None:
    assert build_connect_args(_settings(db_connect_timeout_seconds=2.5))["timeout"] == 2.5


def test_statement_timeout_is_a_server_setting_in_milliseconds() -> None:
    args = build_connect_args(_settings(db_statement_timeout_ms=7500))
    # String, e em milissegundos: e o formato que o GUC do Postgres aceita.
    assert args["server_settings"] == {"statement_timeout": "7500"}


def test_command_timeout_is_derived_with_a_margin_over_the_server_timeout() -> None:
    """O teto do cliente fica ACIMA do do servidor, nao abaixo.

    Assim, em operacao normal, quem cancela e o servidor (sqlstate 57014, com
    mensagem explicativa) e nao o cliente.
    """
    args = build_connect_args(_settings(db_statement_timeout_ms=3000))
    assert args["command_timeout"] == 4.0
    assert args["command_timeout"] > 3000 / 1000

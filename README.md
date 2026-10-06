# async-event-worker

Arquitetura orientada a eventos e mensageria assincrona em Python 3.12: uma API publisher
(FastAPI) que aceita eventos e responde `202 Accepted`, um worker consumidor AMQP que processa
as mensagens em segundo plano e persiste o resultado no PostgreSQL, com retry automatico e
Dead Letter Exchange no RabbitMQ.

> Documentacao completa (topologia, diagramas, exemplos `curl`, receita da DLX) sera escrita na
> etapa final do projeto. Este README cobre apenas o que ja existe.

## Subir a stack

```bash
cp .env.example .env     # opcional: os defaults ja funcionam para dev local
docker compose up -d
```

As credenciais default do `docker-compose.yml` (`app/app`, `guest/guest`) servem **apenas para
desenvolvimento local**. Em qualquer outro ambiente, defina as variaveis de ambiente
correspondentes.

## Comandos

| Alvo do Makefile | Equivalente direto |
|---|---|
| `make up` | `docker compose up -d --build` |
| `make down` | `docker compose down` |
| `make ps` | `docker compose ps` |
| `make logs` | `docker compose logs -f` |
| `make migrate` | `docker compose exec api alembic upgrade head` |
| `make test` | `python -m pytest -q` |
| `make test-integration` | `python -m pytest -m integration -q` |
| `make lint` | `python -m ruff check .` |
| `make reset-broker` | `docker compose rm -sf rabbitmq && docker volume rm <proj>_rabbitmqdata` |

`make` pode nao existir no Windows: use a coluna da direita.

## Notas de ambiente

- O runtime canonico e a imagem `python:3.12-slim`. O `pyproject.toml` exige
  `>=3.12,<3.13`, portanto Pythons mais novos do host nao instalam o projeto.
- Argumentos de fila do RabbitMQ sao imutaveis: se mudar nome/argumento de topologia, rode
  `make reset-broker` antes de subir novamente, senao a declaracao falha com
  `PRECONDITION_FAILED (406)`.

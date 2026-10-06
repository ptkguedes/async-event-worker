# async-event-worker

Arquitetura orientada a eventos e mensageria assincrona em Python 3.12: uma **API publisher**
(FastAPI) que aceita eventos e responde `202 Accepted` imediatamente, um **worker consumidor**
AMQP que processa as mensagens em segundo plano e persiste o resultado no PostgreSQL, com
**retry automatico**, **Dead Letter Exchange (DLX)** e **garantia de idempotencia**.

Stack: Python 3.12, FastAPI, RabbitMQ (aio-pika), PostgreSQL (SQLAlchemy 2.0 async + Alembic),
Pytest e Docker Compose.

---

## 1. Visao geral da arquitetura

O ponto central e que a API **nao executa regra de negocio**. Ela valida o payload, publica a
mensagem e devolve o `task_id` na mesma hora; quem trabalha e o worker, em outro processo.

| Componente | Responsabilidade |
|---|---|
| `app/api` (FastAPI) | Valida via Pydantic, publica na `tasks.exchange` e responde `202` com o `task_id`. Tambem consulta o resultado (`GET /api/v1/tasks/{task_id}`) e o `GET /health`. |
| `app/worker` (consumer) | Consome a fila `tasks` com ack manual, simula o processamento, grava o resultado no Postgres e decide entre retentar ou mandar para a DLX. |
| `app/core` | Configuracao unica (`config.py`), topologia unica (`topology.py`), conexao/publisher (`broker.py`), logging JSON, constantes e excecoes. |
| `app/db` | Modelo `Task` (SQLAlchemy 2.0 declarativo), sessao async e repositorio com o claim idempotente. |
| RabbitMQ | Transporte, atraso entre retentativas (via TTL) e dead lettering nativo. |
| PostgreSQL | Estado e resultado das tasks; a PRIMARY KEY `task_id` e o que garante a idempotencia. |

Estrutura de pastas:

```
app/
  api/       main.py, deps.py, schemas.py, v1/tasks.py, v1/health.py
  worker/    main.py (entrypoint + callback), handlers.py, retry.py
  core/      config.py, topology.py, broker.py, logging.py, constants.py, exceptions.py
  db/        base.py, models.py, session.py, repository.py
alembic/     env.py (async) + versions/0001_create_tasks_table.py
tests/
  unit/          6 arquivos, sem servico externo (fakes em memoria)
  integration/   conftest.py + os 3 fluxos (RabbitMQ e Postgres reais)
```

### Fluxo das mensagens

```mermaid
flowchart LR
    client([Cliente HTTP])
    api["API FastAPI<br/>POST /api/v1/tasks<br/>202 Accepted"]
    tx{{"tasks.exchange<br/>(direct, durable)"}}
    tq[["fila tasks"]]
    worker["Worker<br/>consumer com ack manual"]
    db[("PostgreSQL<br/>tabela tasks")]
    rx{{"tasks.retry.exchange<br/>(DLX nativa da fila tasks)"}}
    rq[["fila tasks.retry<br/>x-message-ttl = 5000ms"]]
    dx{{"tasks.dlx.exchange"}}
    dq[["fila dlx_tasks<br/>(terminal)"]]

    client -->|"event_type + payload"| api
    api -->|"publish PERSISTENT<br/>rk: tasks.process"| tx
    tx --> tq
    tq -->|"deliver"| worker
    worker -->|"sucesso: ack<br/>status COMPLETED + result"| db
    worker -.->|"falha: nack(requeue=false)<br/>status FAILED"| rx
    rx --> rq
    rq -.->|"expira pelo TTL<br/>rk: tasks.process"| tx
    worker ==>|"retentativas esgotadas:<br/>publish + ack na original"| dx
    dx ==> dq
```

Linha cheia = caminho felix. Linha tracejada = retry. Linha grossa = dead letter.

---

## 2. Topologia do RabbitMQ

Declarada em um unico lugar (`app/core/topology.py`), pela mesma funcao
`declare_topology()` que a API e o worker chamam no startup -- os dois processos nunca
divergem. Todos os nomes e valores vem de `Settings` (`app/core/config.py`).

### Exchanges

| Nome | Tipo | Durable | Para que serve |
|---|---|---|---|
| `tasks.exchange` | `direct` | sim | Entrada do trabalho. Recebe do publisher e tambem o retorno da fila de retry. |
| `tasks.retry.exchange` | `direct` | sim | Destino do dead lettering **nativo** da fila `tasks`. |
| `tasks.dlx.exchange` | `direct` | sim | Destino final, usado explicitamente pelo worker quando as retentativas acabam. |

### Filas e TODOS os argumentos

| Fila | `x-queue-type` | `x-message-ttl` | `x-dead-letter-exchange` | `x-dead-letter-routing-key` | Durable |
|---|---|---|---|---|---|
| `tasks` | `classic` | — | `tasks.retry.exchange` | `tasks.retry` | sim |
| `tasks.retry` | `classic` | `5000` (`RETRY_TTL_MS`) | `tasks.exchange` | `tasks.process` | sim |
| `dlx_tasks` | `classic` | — | **nenhum (de proposito)** | — | sim |

`dlx_tasks` e **terminal**: nao tem `x-dead-letter-exchange`. Se tivesse, a mensagem morta
voltaria a circular pela topologia.

### Bindings

| Fila | Exchange | Routing key |
|---|---|---|
| `tasks` | `tasks.exchange` | `tasks.process` |
| `tasks.retry` | `tasks.retry.exchange` | `tasks.retry` |
| `dlx_tasks` | `tasks.dlx.exchange` | `tasks.dead` |

### Regra de contagem das retentativas (header `x-death`)

Nao existe loop de retry em codigo de aplicacao. O atraso e a recontagem vem da topologia:

1. O worker falha e da `nack(requeue=False)`.
2. A DLX **nativa** da fila `tasks` manda a mensagem para `tasks.retry.exchange` e o RabbitMQ
   acrescenta/incrementa uma entrada no header `x-death` com
   `queue: tasks`, `reason: rejected`, `count: N`.
3. Na fila `tasks.retry` a mensagem espera o `x-message-ttl`, expira e e dead-lettered de volta
   para `tasks.exchange` com a routing key `tasks.process`.

`app/worker/retry.py` calcula `retry_count` somando o campo `count` das entradas de `x-death`
**cuja fila e a fila de trabalho e cujo `reason` e `rejected`** (entradas com `reason: expired`,
geradas pelo TTL da fila de retry, sao ignoradas). Na primeira entrega o header nao existe e
`retry_count = 0`.

**Decisao explicita: `TASK_MAX_RETRIES=3` significa 3 RETENTATIVAS, ou seja 4 processamentos no
total.** `decide(retry_count, max_retries)` devolve `DEAD_LETTER` quando
`retry_count >= max_retries`:

| Entrega | `retry_count` | `attempts` gravado | Decisao em caso de falha |
|---|---|---|---|
| 1a (original) | 0 | 1 | `RETRY` |
| 2a | 1 | 2 | `RETRY` |
| 3a | 2 | 3 | `RETRY` |
| 4a | 3 | 4 | `DEAD_LETTER` |

Resultado final de uma task que falha sempre: **1 mensagem em `dlx_tasks`** e a linha no banco
com `status = FAILED` e `attempts = 4`.

### Headers gravados na mensagem morta

| Header | Valor |
|---|---|
| `x-retry-count` | `3` (retentativas consumidas) |
| `x-attempts` | `4` (processamentos no total) |
| `x-failure-reason` | mensagem da excecao que derrubou o processamento |
| `x-original-exchange` | `tasks.exchange` |
| `x-original-routing-key` | `tasks.process` |
| `x-failed-at` | timestamp ISO-8601 UTC |

O **corpo original** da mensagem e preservado intacto, para permitir reprocessamento manual.

### Garantia de idempotencia

`task_id` e a PRIMARY KEY da tabela `tasks`. Antes de qualquer regra de negocio, o worker
executa um claim atomico em uma unica instrucao:

```sql
INSERT INTO tasks (...) VALUES (...)
ON CONFLICT (task_id) DO UPDATE SET status = 'PROCESSING', attempts = :attempts, updated_at = now()
WHERE tasks.status <> 'COMPLETED'
RETURNING task_id;
```

Se nada e retornado, a linha existente ja estava `COMPLETED`: a entrega e uma duplicata, o
worker **nao chama o processamento**, apenas da ack. Nao existe janela de corrida entre ler e
escrever, porque leitura e escrita sao a mesma instrucao.

---

## 3. Subindo a stack com Docker Compose

```bash
cp .env.example .env     # opcional: os defaults ja funcionam para dev local
docker compose up -d     # ou: make up
docker compose ps        # ou: make ps
```

Os 4 containers:

| Servico | Imagem | Portas no host | Observacao |
|---|---|---|---|
| `postgres` | `postgres:17-alpine` | `5432` | volume `pgdata`, healthcheck com `pg_isready` |
| `rabbitmq` | `rabbitmq:4.3-management-alpine` | `5672` (AMQP), `15672` (UI) | volume `rabbitmqdata`, healthcheck com `rabbitmq-diagnostics` |
| `api` | build local | `8000` | sobe depois dos dois healthy |
| `worker` | build local (mesma imagem) | — | `restart: unless-stopped`, `stop_grace_period: 20s` |

**Migrations:** o `command` do servico `api` roda `alembic upgrade head` antes do uvicorn, ou
seja, subir o Compose ja aplica o schema. Para rodar manualmente:

```bash
docker compose exec api alembic upgrade head     # ou: make migrate
```

Enderecos uteis:

- API: <http://localhost:8000> (docs em `/docs`)
- RabbitMQ Management: <http://localhost:15672> (usuario/senha default `guest`/`guest`)
- PostgreSQL: `localhost:5432` (`app`/`app`, banco `async_event_worker`)

---

## 4. Usando a API

### POST /api/v1/tasks -- publica um evento (202 Accepted)

```bash
curl -i -X POST localhost:8000/api/v1/tasks \
  -H "content-type: application/json" \
  -d '{"event_type":"order.created","payload":{"order_id":42}}'
```

```http
HTTP/1.1 202 Accepted
```

```json
{
  "task_id": "9f1c5b1e-6f0a-4a3b-8f2e-1d9f0b8c7a65",
  "status": "PENDING",
  "accepted_at": "2024-05-01T12:00:00.000000+00:00"
}
```

O `task_id` e gerado pela API, mas pode ser **informado pelo cliente** -- util para
idempotencia ponta a ponta (reenviar a mesma requisicao nao cria uma segunda task):

```bash
curl -s -X POST localhost:8000/api/v1/tasks \
  -H "content-type: application/json" \
  -d '{"task_id":"11111111-1111-1111-1111-111111111111","event_type":"order.created","payload":{"order_id":42}}'
```

### GET /api/v1/tasks/{task_id} -- consulta o resultado

```bash
curl -s localhost:8000/api/v1/tasks/11111111-1111-1111-1111-111111111111
```

```json
{
  "task_id": "11111111-1111-1111-1111-111111111111",
  "event_type": "order.created",
  "payload": {"order_id": 42},
  "status": "COMPLETED",
  "attempts": 1,
  "result": {"processed_at": "2024-05-01T12:00:00.123456+00:00", "event_type": "order.created", "echo": {"order_id": 42}},
  "error": null,
  "created_at": "2024-05-01T12:00:00.000000+00:00",
  "updated_at": "2024-05-01T12:00:00.500000+00:00"
}
```

Responde `404` quando o `task_id` nao existe (inclusive no intervalo entre o `202` e o worker
reservar a task).

### GET /health

```bash
curl -s localhost:8000/health
```

```json
{"status": "ok", "database": "ok", "broker": "ok"}
```

Devolve `503` identificando o componente degradado quando o banco ou o broker nao respondem.

---

## 5. Receita: ver uma mensagem cair em `dlx_tasks`

O payload com `"force_failure": true` faz `simulate_processing` levantar
`TaskProcessingError` em todas as tentativas -- e o gatilho deliberado para demonstrar o
caminho de falha sem alterar codigo.

```bash
# 1) publique a task condenada
curl -s -X POST localhost:8000/api/v1/tasks \
  -H "content-type: application/json" \
  -d '{"event_type":"demo.fail","payload":{"force_failure":true}}'

# 2) acompanhe o worker: 3 linhas "task rejected for retry" (retry_count 0, 1, 2),
#    com ~5s de intervalo (o x-message-ttl), e depois "task moved to dead letter queue"
docker compose logs -f worker     # ou: make logs

# 3) confira as filas (~15s depois): dlx_tasks = 1, tasks = 0, tasks.retry = 0
docker compose exec rabbitmq rabbitmqctl list_queues name messages     # ou: make dlq
```

Na UI em <http://localhost:15672> (`guest`/`guest`), aba **Queues** -> `dlx_tasks` ->
**Get messages** mostra o corpo original e os headers `x-retry-count: 3`, `x-attempts: 4`,
`x-failure-reason`, `x-original-exchange`, `x-original-routing-key` e `x-failed-at`.

No banco, a linha correspondente fica `FAILED` com `attempts = 4`:

```bash
docker compose exec postgres psql -U app -d async_event_worker \
  -c "select task_id, status, attempts, error from tasks order by updated_at desc limit 1;"
```

---

## 6. Testes

Duas suites separadas pelo marker `integration`:

| Suite | Onde | Precisa de servico? | Comando |
|---|---|---|---|
| Unitaria (default) | `tests/unit` | nao (fakes em memoria) | `python -m pytest -q` |
| Integracao | `tests/integration` | **sim**: Postgres + RabbitMQ | `python -m pytest -m integration -q` |
| Tudo | — | sim | `python -m pytest -m '' -q` |

O `addopts` do `pyproject.toml` ja traz `-m 'not integration'`, portanto `pytest -q` roda
somente a suite unitaria.

### Suite de integracao

```bash
docker compose up -d postgres rabbitmq
python -m pytest -m integration -q
```

Detalhes importantes:

- Os testes **pulam com mensagem explicativa** (nao falham) se o Postgres ou o RabbitMQ nao
  responderem em 5s.
- **Atencao:** o isolamento por `topology_prefix` vale para as filas, nao para o banco. A suite
  roda `TRUNCATE TABLE tasks` antes de cada teste no banco apontado por `TEST_DATABASE_URL` --
  que por default e o mesmo `async_event_worker` do Compose. Se quiser preservar os dados de
  dev, aponte `TEST_DATABASE_URL` para outro banco antes de rodar a integracao.
- O schema e aplicado pelo proprio `conftest.py` com `alembic upgrade head` -- isso valida a
  migration de verdade.
- A topologia de teste usa `topology_prefix="test_"` (`test_tasks`, `test_tasks.retry`,
  `test_dlx_tasks`) com `RETRY_TTL_MS=500`. Sem o prefixo, o worker que roda no Compose
  disputaria as mensagens com o worker in-process da suite.
- URLs sobrescreviveis por ambiente: `TEST_DATABASE_URL`
  (default `postgresql+asyncpg://app:app@localhost:5432/async_event_worker`) e `TEST_AMQP_URL`
  (default `amqp://guest:guest@localhost:5672/`).

Os tres fluxos cobertos:

| Arquivo | Fluxo | O que prova |
|---|---|---|
| `test_success_flow.py` | sucesso | a task chega a `COMPLETED` com `result` preenchido, `error` nulo, `attempts == 1` e nada em `test_dlx_tasks`. |
| `test_idempotency.py` | idempotencia | duas entregas do mesmo `task_id` produzem **1 linha**, `attempts == 1`, e o `result` e o da primeira entrega (a duplicata recebeu ack sem reexecutar). |
| `test_dlx_routing.py` | DLX | exatamente **1 mensagem** em `test_dlx_tasks` com `x-attempts == 4` e `x-retry-count == 3`, linha `FAILED` com `attempts == 4`, filas de trabalho e de retry vazias. |

Lint e formatacao:

```bash
python -m ruff check .     # ou: make lint
python -m ruff format .    # ou: make fmt
```

---

## 7. Alvos do Makefile

`make` pode nao existir no host (Windows, por exemplo): use a coluna da direita.

| Alvo | Comando equivalente |
|---|---|
| `make up` | `docker compose up -d --build` |
| `make down` | `docker compose down` |
| `make ps` | `docker compose ps` |
| `make logs` | `docker compose logs -f` |
| `make migrate` | `docker compose exec api alembic upgrade head` |
| `make revision m="msg"` | `docker compose exec api alembic revision --autogenerate -m "msg"` |
| `make test` | `python -m pytest -q` |
| `make test-integration` | `python -m pytest -m integration -q` |
| `make test-all` | `python -m pytest -m '' -q` |
| `make lint` | `python -m ruff check .` |
| `make fmt` | `python -m ruff format . && python -m ruff check . --fix` |
| `make reset-broker` | `docker compose rm -sf rabbitmq && docker volume rm <projeto>_rabbitmqdata` |
| `make dlq` | `docker compose exec rabbitmq rabbitmqctl list_queues name messages` |
| `make shell` | `docker compose exec api bash` |

---

## 8. Configuracao

Todas as variaveis sao lidas por `app/core/config.py` (pydantic-settings) do ambiente ou de um
arquivo `.env`. Nenhum nome de fila, exchange, routing key, TTL ou limite aparece como literal
fora desse modulo. Os principais:

| Variavel | Default | Para que serve |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://app:app@postgres:5432/async_event_worker` | conexao async do SQLAlchemy |
| `AMQP_URL` | `amqp://guest:guest@rabbitmq:5672/` | conexao do RabbitMQ |
| `TASK_MAX_RETRIES` | `3` | retentativas antes da DLX (4 processamentos) |
| `RETRY_TTL_MS` | `5000` | atraso entre retentativas (`x-message-ttl`) |
| `WORKER_PREFETCH_COUNT` | `10` | mensagens em voo por consumidor (QoS) |
| `PROCESSING_DELAY_SECONDS` | `0.5` | duracao do processamento simulado |
| `TOPOLOGY_PREFIX` | vazio | prefixo de todos os nomes de fila/exchange (a suite de integracao usa `test_`) |
| `LOG_LEVEL` | `INFO` | nivel do logging JSON em stdout |

A lista completa esta em `.env.example`.

---

## 9. Notas operacionais

### Garantias de durabilidade e entrega

- **Exchanges e filas duraveis**: sobrevivem ao restart do broker.
- **`delivery_mode=PERSISTENT`** em toda publicacao (inclusive na DLX): a mensagem e gravada em
  disco, nao so na memoria.
- **Publisher confirms** (`channel(publisher_confirms=True)`): o `await publish(...)` so retorna
  depois do ack do broker, portanto a API nunca responde `202` para uma mensagem que o RabbitMQ
  nao aceitou.
- **QoS / prefetch** (`set_qos(prefetch_count=...)`): limita as mensagens em voo por consumidor
  e permite escalar o worker horizontalmente sem que um unico processo acumule trabalho.
- **Ack manual** (sem `message.process()`): o destino de uma mensagem que falhou depende da
  contagem de retentativas, entao o `ack`/`nack` e explicito. Processo morto no meio do
  processamento = mensagem reentregue, e o claim idempotente evita efeito duplicado.
- **Resultado persistido antes do ack**: o `COMPLETED` e gravado no Postgres antes de confirmar
  a mensagem.

### Credenciais

As credenciais default do `docker-compose.yml` (`app`/`app` no Postgres, `guest`/`guest` no
RabbitMQ) servem **apenas para desenvolvimento local**. O arquivo usa exclusivamente
`${VAR:-default}`, sem segredo literal: em qualquer outro cenario defina
`POSTGRES_USER`, `POSTGRES_PASSWORD`, `RABBITMQ_DEFAULT_USER`, `RABBITMQ_DEFAULT_PASS`,
`DATABASE_URL` e `AMQP_URL` pelo ambiente (ou por um gerenciador de segredos) e nunca commite o
`.env`.

### Armadilha: `PRECONDITION_FAILED (406)`

Argumentos de fila sao **imutaveis** no RabbitMQ. Se voce mudar `RETRY_TTL_MS`, uma
`x-dead-letter-*` ou qualquer outro argumento de uma fila que **ja existe**, a declaracao falha
com `PRECONDITION_FAILED (406)` -- `declare_topology()` converte isso em `TopologyError` com a
explicacao. Duas saidas:

```bash
make reset-broker     # apaga o volume do RabbitMQ e recria as filas do zero
```

ou use um `TOPOLOGY_PREFIX` diferente para criar um conjunto novo de filas.

### Ambiente: caminho WSL/UNC

Quando o projeto vive em um caminho `\\wsl.localhost\...` acessado do Windows, o bind mount
`.:/app` dos servicos `api` e `worker` pode falhar com
`accessing specified distro mount service: ... ubuntu.sock: no such file or directory`. Nesse
caso, **rode o Compose de dentro do WSL** (onde o caminho e `/home/<user>/...` nativo), com a
integracao WSL do Docker Desktop habilitada. Alternativas: habilitar a integracao WSL em
Docker Desktop > Settings > Resources > WSL integration, ou remover temporariamente o bind
mount (a imagem ja contem o codigo).

O mesmo vale para o Python: o `pyproject.toml` exige `>=3.12,<3.13`, portanto um Python mais
novo no host nao instala o projeto. Use a imagem `python:3.12-slim` (runtime canonico) ou um
virtualenv com Python 3.12:

```bash
python3.12 -m venv .venv && .venv/bin/python -m pip install -e ".[dev]"
```

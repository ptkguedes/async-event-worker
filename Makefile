.DEFAULT_GOAL := help
.PHONY: help up down logs ps migrate revision test test-integration test-all lint fmt reset-broker dlq shell

help: ## Lista os alvos disponiveis
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

up: ## Sobe os 4 servicos em background (build incluso)
	docker compose up -d --build

down: ## Para os servicos e remove os containers
	docker compose down

logs: ## Acompanha os logs de todos os servicos
	docker compose logs -f

ps: ## Mostra o estado dos servicos
	docker compose ps

migrate: ## Aplica as migrations do Alembic
	docker compose exec api alembic upgrade head

revision: ## Cria uma migration (uso: make revision m="mensagem")
	docker compose exec api alembic revision --autogenerate -m "$(m)"

test: ## Roda a suite unitaria
	python -m pytest -q

test-integration: ## Roda a suite de integracao (exige postgres e rabbitmq no ar)
	python -m pytest -m integration -q

test-all: ## Roda todas as suites
	python -m pytest -m '' -q

lint: ## Verifica o lint com ruff
	python -m ruff check .

fmt: ## Formata e corrige o que o ruff consegue automaticamente
	python -m ruff format .
	python -m ruff check . --fix

reset-broker: ## Apaga o volume do RabbitMQ (necessario ao mudar argumentos de fila)
	docker compose rm -sf rabbitmq
	docker volume ls -q --filter name=rabbitmqdata | xargs -r docker volume rm

dlq: ## Mostra a contagem de mensagens nas filas (inclui dlx_tasks)
	docker compose exec rabbitmq rabbitmqctl list_queues name messages

shell: ## Abre um shell no container da api
	docker compose exec api bash

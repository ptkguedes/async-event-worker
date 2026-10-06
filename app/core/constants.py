"""Nomes de headers AMQP e chaves de payload usados pelo fluxo de retry/DLX."""

# Header nativo que o RabbitMQ grava quando uma mensagem e dead-lettered.
X_DEATH_HEADER = "x-death"

# Headers que o worker acrescenta ao publicar na dead letter queue.
X_RETRY_COUNT_HEADER = "x-retry-count"
X_ATTEMPTS_HEADER = "x-attempts"
X_FAILURE_REASON_HEADER = "x-failure-reason"
X_ORIGINAL_EXCHANGE_HEADER = "x-original-exchange"
X_ORIGINAL_ROUTING_KEY_HEADER = "x-original-routing-key"
X_FAILED_AT_HEADER = "x-failed-at"

# Chave de payload que forca a falha do processamento (usada em demos e testes).
FORCE_FAILURE_KEY = "force_failure"

# Valor de 'reason' em x-death que indica nack/reject (e nao expiracao de TTL).
DEATH_REASON_REJECTED = "rejected"

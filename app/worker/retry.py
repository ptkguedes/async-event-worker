"""Politica de retentativa derivada do header nativo `x-death`.

Modulo 100% puro: nenhuma chamada ao broker, ao banco ou ao relogio. Toda a
decisao de "retentar" ou "mandar para a dead letter queue" sai daqui, o que
torna o comportamento testavel sem RabbitMQ.

Como o RabbitMQ conta as tentativas: quando o worker da `nack(requeue=False)`,
a mensagem e dead-lettered para a exchange de retry e o broker acrescenta (ou
incrementa) uma entrada em `x-death` com `queue` = fila de origem e
`reason` = "rejected". A fila de retry devolve a mensagem para a fila de
trabalho pelo `x-message-ttl` -- essa volta gera uma entrada com
`reason` = "expired", que NAO conta como retentativa (senao cada ciclo
contaria duas vezes).

Com `max_retries=3` a mensagem e entregue 4 vezes na fila `tasks`
(1 entrega original + 3 retentativas) antes de ir para `dlx_tasks`. Nao existe
loop de retry em codigo de aplicacao: o atraso e a reentrega vem dos argumentos
das filas.
"""

from typing import Any, Literal

from app.core.constants import DEATH_REASON_REJECTED, X_DEATH_HEADER

RetryDecision = Literal["RETRY", "DEAD_LETTER"]

# Campos da entrada de x-death (nomes do protocolo, nao configuracao).
_DEATH_QUEUE_FIELD = "queue"
_DEATH_REASON_FIELD = "reason"
_DEATH_COUNT_FIELD = "count"


def retry_count_from_headers(headers: dict[str, Any] | None, source_queue: str) -> int:
    """Quantas vezes a mensagem ja foi rejeitada na fila `source_queue`.

    Soma o campo `count` das entradas de `x-death` cuja `queue` e a fila de
    trabalho e cujo `reason` e "rejected". Devolve 0 quando o header esta
    ausente, vazio, malformado ou so contem entradas de expiracao de TTL --
    isto e, na primeira entrega o resultado e sempre 0.
    """
    if not headers:
        return 0

    deaths = headers.get(X_DEATH_HEADER)
    if not isinstance(deaths, list):
        return 0

    total = 0
    for death in deaths:
        if not isinstance(death, dict):
            continue
        if death.get(_DEATH_QUEUE_FIELD) != source_queue:
            continue
        if death.get(_DEATH_REASON_FIELD) != DEATH_REASON_REJECTED:
            continue
        count = death.get(_DEATH_COUNT_FIELD, 0)
        if isinstance(count, int):
            total += count
    return total


def decide(retry_count: int, max_retries: int) -> RetryDecision:
    """Decide o destino da mensagem que acabou de falhar.

    Devolve "DEAD_LETTER" quando as retentativas acabaram
    (`retry_count >= max_retries`) e "RETRY" caso contrario. Com
    `max_retries=3`: 0, 1 e 2 retentam; 3 (ou mais) vai para a dead letter queue.
    """
    return "DEAD_LETTER" if retry_count >= max_retries else "RETRY"

"""Excecoes de dominio da aplicacao."""


class AppError(Exception):
    """Classe base de todas as excecoes da aplicacao."""


class TaskProcessingError(AppError):
    """O processamento da task falhou e a mensagem deve ser reentregue ou dead-lettered."""


class TopologyError(AppError):
    """Falha ao declarar ou validar a topologia do RabbitMQ."""


class DuplicateTaskError(AppError):
    """A task_id informada ja foi processada com sucesso."""

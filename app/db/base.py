"""Base declarativa do SQLAlchemy 2.0."""

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

# Convencao de nomes explicita: deixa os nomes de indices/constraints estaveis
# entre o modelo e as migrations do Alembic.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Classe base de todos os modelos ORM."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)

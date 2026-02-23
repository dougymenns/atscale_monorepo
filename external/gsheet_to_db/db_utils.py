from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import logging

logger = logging.getLogger(__name__)


def db_connection(
    DB_USER: str,
    DB_PASSWORD: str,
    ENDPOINT: str,
    DB_NAME: str,
    db_type: str = 'POSTGRESQL'
) -> Engine:
    """
    Create a SQLAlchemy database engine for PostgreSQL or Amazon Redshift.
    """
    try:
        if db_type.upper() == 'POSTGRESQL':
            db_uri = (
                f'postgresql://{DB_USER}:{DB_PASSWORD}@{ENDPOINT}:5432/{DB_NAME}'
            )
        elif db_type.upper() == 'REDSHIFT':
            db_uri = (
                f'redshift+psycopg2://{DB_USER}:{DB_PASSWORD}'
                f'@{ENDPOINT}:5439/{DB_NAME}'
            )
        else:
            raise ValueError(
                f"Unsupported db type: '{db_type}'. Use POSTGRESQL or REDSHIFT."
            )

        engine = create_engine(db_uri)
        return engine

    except Exception as ex:
        logger.error(
            "Failed to create database engine for %s: %s",
            db_type.upper(),
            ex,
        )
        raise

import logging
from typing import Optional, Union
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def db_connection(
    DB_USER: str,
    DB_PASSWORD: str,
    ENDPOINT: str,
    DB_NAME: str,
    db_type: str = 'POSTGRESQL',
    PORT: Optional[Union[str, int]] = None,
) -> Engine:
    """
    Create a SQLAlchemy database engine for PostgreSQL or Amazon Redshift.

    This function dynamically builds a connection URI based on the database type 
    and the environment context. If the environment user is 'dougymenns', it defaults 
    to local PostgreSQL credentials for development.

    Parameters
    ----------
    DB_USER : str
        The database username.
    DB_PASSWORD : str
        The database password.
    ENDPOINT : str
        The database endpoint or host address.
    DB_NAME : str
        The target database name.
    db_type : str, optional
        The type of database connection to create. 
        Supported options: 'POSTGRESQL', 'REDSHIFT'.
        Default is 'POSTGRESQL'.

    Returns
    -------
    sqlalchemy.engine.Engine
        A SQLAlchemy Engine object that manages database connections.

    Raises
    ------
    ValueError
        If `db_type` is not one of the supported options.
    SQLAlchemyError
        If the engine creation fails.

    Examples
    --------
    >>> engine = db_connection('user', 'password', 'example.redshift.amazonaws.com', 'analytics', 'REDSHIFT')
    >>> df = pd.read_sql("SELECT * FROM my_table;", engine)
    """
    try:
        if db_type.upper() == 'POSTGRESQL':
            port = int(PORT) if PORT is not None else 5432
            db_uri = f'postgresql://{DB_USER}:{DB_PASSWORD}@{ENDPOINT}:{port}/{DB_NAME}'
        elif db_type.upper() == 'REDSHIFT':
            port = int(PORT) if PORT is not None else 5439
            db_uri = f'redshift+psycopg2://{DB_USER}:{DB_PASSWORD}@{ENDPOINT}:{port}/{DB_NAME}'
        else:
            raise ValueError(f"CUSTOM INFO: <xxxxx Unsupported database type: '{db_type}'. Use 'POSTGRESQL' or 'REDSHIFT'. xxxxx>")

        # Create SQLAlchemy engine
        engine = create_engine(db_uri)
        return engine

    except Exception as ex:
        logger.error(f"CUSTOM INFO: <xxxxx Failed to create database engine for {db_type.upper()}: {ex} xxxxx>")
        raise
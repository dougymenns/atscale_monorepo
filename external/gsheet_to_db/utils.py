import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _pandas_dtype_to_pg(dtype) -> str:
    """Map pandas dtype to PostgreSQL type. Default to TEXT for safety."""
    name = getattr(dtype, "name", str(dtype))
    if name in ("int64", "int32", "int8"):
        return "BIGINT"
    if name in ("float64", "float32"):
        return "DOUBLE PRECISION"
    if name == "bool":
        return "BOOLEAN"
    if name.startswith("datetime"):
        return "TIMESTAMP"
    if name == "object" or name == "string":
        return "TEXT"
    return "TEXT"


class DB_QUERY_MANAGER:
    """DB manager: batch insert and full-load (replace)."""

    def __init__(self, engine: Engine):
        self.engine = engine

    def create_table_if_not_exists(
        self, df: pd.DataFrame, schema: str, table: str
    ) -> None:
        """Create table from df columns and inferred types if not exists."""
        if df.empty:
            logger.warning("create_table_if_not_exists: empty df, skip.")
            return
        col_defs = []
        for c in df.columns:
            pg_type = _pandas_dtype_to_pg(df[c].dtype)
            col_defs.append(f'"{c}" {pg_type}')
        col_defs.append('load_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        cols_ddl = ", ".join(col_defs)
        create_sql = text(
            f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({cols_ddl})"  # noqa: E501
        )
        with self.engine.begin() as conn:
            conn.execute(create_sql)
            # Add load_dt to existing tables that don't have it
            alter_sql = text(
                f"ALTER TABLE {schema}.{table} "
                "ADD COLUMN IF NOT EXISTS load_dt TIMESTAMP "
                "DEFAULT CURRENT_TIMESTAMP"
            )
            conn.execute(alter_sql)
        logger.info("create_table_if_not_exists: %s.%s ok.", schema, table)

    def batch_insert(
        self, df: pd.DataFrame, schema: str, table: str, replace: bool = False
    ) -> bool:
        """Insert df into Postgres. replace=True: TRUNCATE then insert."""
        if df.empty:
            logger.warning("batch_insert called with empty DataFrame.")
            return False

        try:
            rows = df.to_dict(orient="records")
            cols = df.columns.tolist()
            col_str = ",".join([f'"{c}"' for c in cols])
            param_str = ",".join([f":{c}" for c in cols])

            insert_sql = text(
                f"""
                INSERT INTO {schema}.{table} ({col_str})
                VALUES ({param_str})
                """
            )

            with self.engine.begin() as conn:
                if replace:
                    truncate_sql = text(f"TRUNCATE TABLE {schema}.{table}")
                    conn.execute(truncate_sql)

                result = conn.execute(insert_sql, rows)
                row_count = result.rowcount
                if row_count >= 0:
                    logger.info(
                        "batch_insert successful. Rows affected: %s.", row_count
                    )
                    return True
            return True

        except Exception as ex:
            logger.exception(
                "batch_insert failed: %s", ex
            )
            return False

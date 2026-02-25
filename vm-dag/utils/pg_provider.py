import contextlib
import logging
from typing import Iterator
from airflow.providers.postgres.hooks.postgres import PostgresHook

_logger = logging.getLogger(__name__)

@contextlib.contextmanager
def yield_pg_cursor(
    existing_conn = None,
    pg_target_id: str = "DB_CONN",
) -> Iterator:
    
    actual_conn = existing_conn or PostgresHook(postgres_conn_id=pg_target_id).get_conn()
    is_psycopg3 = hasattr(actual_conn, "row_factory")

    if is_psycopg3:
        from psycopg.rows import dict_row
        cur = actual_conn.cursor(row_factory=dict_row)
    else:
        from psycopg2.extras import RealDictCursor
        cur = actual_conn.cursor(cursor_factory=RealDictCursor)

    try:
        yield cur
        if existing_conn is None:
            actual_conn.commit()
    except Exception as e:
        if existing_conn is None:
            actual_conn.rollback()
        _logger.error("Ошибка в транзакции БД: %s", e)
        raise
    finally:
        cur.close()
        if existing_conn is None:
            actual_conn.close()
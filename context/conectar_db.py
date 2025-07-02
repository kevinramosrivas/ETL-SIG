from contextlib import contextmanager
from env_config import settings
import psycopg2
import psycopg2.extras
# ----------------------------
# Context manager de conexión
# ----------------------------
@contextmanager
def conectar_bd(autocommit: bool = True):
    conn = psycopg2.connect(
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        host=settings.db_host,
        port=settings.db_port,
    )
    try:
        conn.autocommit = autocommit
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        yield conn,cursor
    finally:
        cursor.close()
        conn.close()

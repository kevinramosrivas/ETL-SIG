from contextlib import contextmanager
from typing import Iterator, Optional, Tuple
import psycopg2
import psycopg2.extras
from config.env_config import settings

@contextmanager
def conectar_bd(
    autocommit: bool = True
) -> Iterator[Tuple[psycopg2.extensions.connection, psycopg2.extras.DictCursor]]:
    """
    Context manager para manejar conexiones a PostgreSQL usando psycopg2.

    Args:
        autocommit (bool): si True, la conexión quedará en modo autocommit.

    Yields:
        Tuple[(conn, cursor)]: unión de conexión y cursor en context.
    """
    conn: Optional[psycopg2.extensions.connection] = None
    cursor: Optional[psycopg2.extras.DictCursor] = None

    try:
        # Inicializar la conexión
        conn = psycopg2.connect(
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
            host=settings.db_host,
            port=settings.db_port,
        )
        conn.autocommit = autocommit

        # Crear cursor que devuelve dicts
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SET lc_messages TO 'C';")
        cursor.execute("SET client_encoding TO 'UTF8';")
        yield conn, cursor

    except psycopg2.Error as db_err:
        # Capturamos errores de conexión o de psycopg2
        msg = str(db_err).encode("utf-8", errors="replace").decode("utf-8", errors="replace")
        raise RuntimeError(f"Error en la conexion a BD:\n{msg}") from db_err

    finally:
        # Cerramos cursor y conexión de forma segura
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

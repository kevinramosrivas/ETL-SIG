import psycopg2
import psycopg2.extras
from config.settings_config import settings  # Importar configuración




class DatabaseConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._connection = None
            cls._instance._cursor = None
        return cls._instance

    def connect(self):
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(
                dbname=settings.db_name,
                user=settings.db_user,
                password=settings.db_password,
                host=settings.db_host,
                port=settings.db_port,
            )
            self._connection.autocommit = True
            self._cursor = self._connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return self._cursor

    def close(self):
        if self._connection:
            self._cursor.close()
            self._connection.close()
            self._connection = None
            self._cursor = None


import io
import os
import csv
import time
from io import StringIO
from typing import Any, Callable, Dict, Iterable, List, Optional
from utils.load_tables_config import load_table_configs, load_query_tables
from utils.filter import build_filter
from dbfread2 import DBF
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from config.settings_config import settings
from config.database_config import DatabaseConnection
from utils.list_to_tuple import list_to_tuple_string




# Deshabilitar cache devolviendo siempre None en cache_key_fn
@task(retries=2, retry_delay_seconds=30)
def read_dbf(path: str) -> Iterable[Dict[str, Any]]:
    logger = get_run_logger()
    full_path = os.path.join(settings.data_dir, path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"DBF file not found: {full_path}")
    logger.info(f"Reading DBF: {full_path}")
    return DBF(full_path, record_factory=dict, encoding="cp1252", char_decode_errors="replace")



# Deshabilitar cache para la conexión
@task(retries=3, retry_delay_seconds = 30,name="CONEXION_BD",cache_policy=NO_CACHE)
def connect_db():
    logger = get_run_logger()
    db = DatabaseConnection()
    logger.info(f"Conexion a la base de datos creada")
    return db



# Deshabilitar cache en bulk_load
@task(retries=1, retry_delay_seconds=10)
def bulk_load(
    table: str,
    columns: List[str],
    records: Iterable[Dict[str, Any]],
    field_map: Optional[Dict[str, str]] = None,
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    truncate: bool = False,
):
    logger = get_run_logger()
    start = time.time()

    db = connect_db()
    cursor = db.connect()
    if truncate:
        cursor.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE;")
        cursor.execute(f"ALTER TABLE {table} DISABLE TRIGGER ALL;")

    buf = StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(columns)

    count = 0
    for rec in records:
        if filter_fn and not filter_fn(rec):
            continue
        row = [rec.get(field_map.get(col, col)) for col in columns]
        writer.writerow(row)
        count += 1
    buf.seek(0)

    cols_sql = ", ".join(columns)
    cursor.copy_expert(f"COPY {table} ({cols_sql}) FROM STDIN WITH (FORMAT csv, HEADER TRUE)", buf)

    if truncate:
        cursor.execute(f"ALTER TABLE {table} ENABLE TRIGGER ALL;")

    logger.info(
        f"Loaded {count} rows into {table} in {round(time.time() - start, 2)}s"
    )
    return True
# Deshabilitar cache en bulk_load
@task(retries=2, retry_delay_seconds=10)
def execute_query(sql_query: str):
    db = connect_db()
    cursor = db.connect()
    # Ejecutar consulta y obtener datos
    cursor.execute(sql_query)
    data = cursor.fetchall()
    # Obtener nombres de columnas
    columns = [desc[0] for desc in cursor.description]

    return data,columns

@task(retries=3, retry_delay_seconds=20)
def load_data_table(data:list,columns:list,name_table_target:str):
    db = connect_db()
    cursor = db.connect()
    try:
        # Truncar tabla y deshabilitar triggers
        cursor.execute(f"TRUNCATE TABLE {name_table_target} RESTART IDENTITY CASCADE;")
        cursor.execute(f"ALTER TABLE {name_table_target} DISABLE TRIGGER ALL;")

        # Preparar buffer CSV
        buffer = io.StringIO()
        for row in data:
            # Sustituir None o 'None' por marcador NULL '\\N'
            values = [item if isinstance(item, str) and item not in ('None', '') else ('\\N' if item is None or item == 'None' else str(item)) for item in row]
            line = list_to_tuple_string(values)
            buffer.write(line + "\n")
        buffer.seek(0)

        # Ejecutar COPY con NULL marcador '\\N'
        cols_formatted = ', '.join(columns)
        copy_sql = f"""
            COPY {name_table_target} ({cols_formatted})
            FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '\\N');
        """
        cursor.copy_expert(copy_sql, buffer)

        # Rehabilitar triggers
        cursor.execute(f"ALTER TABLE {name_table_target} ENABLE TRIGGER ALL;")

        print(f"Datos copiados a la tabla {name_table_target} correctamente.")

    except Exception as e:
        print(f"Error al cargar datos en {name_table_target}: {e}")
        cursor.connection.rollback()
        raise


@flow
def extract():
    tables = load_table_configs()
    for cfg in tables:
        read_dbf_task = read_dbf.with_options(name=f"LECTURA-DBF-{cfg.key.upper()}")
        data = read_dbf_task(cfg.path)
        filter_fn = build_filter(cfg.filter_fields) if cfg.filter_fields else None
        bulk_load_task = bulk_load.with_options(name=f"CARGA-{cfg.key.upper()}")
        bulk_load_task(
            table=cfg.table,
            columns=cfg.columns,
            records=data,
            field_map=cfg.field_map,
            filter_fn=filter_fn,
            truncate=cfg.truncate,
        )

@flow()
def transform():
    tables =  load_query_tables()
    rows_tables = []
    for table in tables:
        data, columns = execute_query(table.query)
        rows_tables.append(
            {
                "table":table.table,
                "data":data,
                "columns":columns
            }
        )
    return rows_tables
@flow()
def load(rows_table: List[Dict]):
    for table in rows_tables:
        load_data_table(table["data"],table["columns"],table["table"])


if __name__ == "__main__":
    extract()
    rows_tables = transform()
    load(rows_tables)

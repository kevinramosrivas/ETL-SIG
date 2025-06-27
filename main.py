import io
import os
import csv
import shutil
import tempfile
import time
from io import StringIO
from typing import Any, Callable, Dict, Iterable, List, Optional
from contextlib import contextmanager

from utils.load_tables_config import load_table_configs, load_query_tables
from utils.filter import build_filter
from dbfread2 import DBF
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from config.settings_config import settings

import psycopg2
import psycopg2.extras
# import smbclient
# import rarfile


# ----------------------------
# Context manager de conexión
# ----------------------------
@contextmanager
def db_cursor(autocommit: bool = True):
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
        yield cursor
    finally:
        cursor.close()
        conn.close()


# --------------------------------
# Tarea para copiar archivos SMB
# --------------------------------
@task()
def copy_file_from_share():
    logger = get_run_logger()
    logger.info("Iniciando copia de archivo desde recurso compartido")
    try:
        servidor = fr"{settings.quipushare}"
        recurso = settings.recurso
        archivo_nombre = settings.file_name
        ruta_remota = fr"\\{servidor}\{recurso}\{archivo_nombre}"
        ruta_local = os.path.join(settings.path_local, archivo_nombre)

        smbclient.register_session(
            servidor,
            username=settings.share_username,
            password=settings.share_password,
        )
        with smbclient.open_file(ruta_remota, mode='rb') as archivo_remoto:
            with open(ruta_local, mode='wb') as archivo_local:
                shutil.copyfileobj(archivo_remoto, archivo_local)

        logger.info(f"Archivo copiado a: {ruta_local}")
    except Exception as e:
        logger.error(f"Error al copiar el archivo: {e}")
    return True


# ----------------------------
# Tarea para descomprimir RAR
# ----------------------------
@task(name="DESCOMPRIMIR-RAR")
def descomprime_rar():
    logger = get_run_logger()
    logger.info("Iniciando descompresión de archivo RAR")
    rarfile.UNRAR_TOOL = "UnRAR"
    try:
        ruta = settings.path_local
        if os.path.exists(ruta):
            with rarfile.RarFile(os.path.join(ruta, "DATA.rar")) as rf:
                rf.extractall(path=settings.path_extract)
                logger.info("Archivo descomprimido correctamente.")
        else:
            logger.error("No se encontró la ruta especificada.")
    except Exception as e:
        logger.error(f"Error al descomprimir el archivo: {e}")
    return True


# -------------------------------
# Tarea para leer archivos DBF
# -------------------------------
@task(retries=2, retry_delay_seconds=30)
def read_dbf(path: str) -> Iterable[Dict[str, Any]]:
    logger = get_run_logger()
    logger.info(f"Iniciando lectura de DBF: {path}")
    full_path = os.path.join(settings.data_dir, path)
    if not os.path.exists(full_path):
        logger.error(f"Archivo DBF no encontrado: {full_path}")
        raise FileNotFoundError(f"DBF no encontrado: {full_path}")
    records = DBF(full_path, record_factory=dict, encoding="cp1252", char_decode_errors="replace")
    return records


# ------------------------------
# Tarea para carga masiva (COPY)
# ------------------------------
@task(retries=1, retry_delay_seconds=10, log_prints=False)
def bulk_load(
    table: str,
    columns: List[str],
    records: Iterable[Dict[str, Any]],
    field_map: Optional[Dict[str, str]] = None,
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    truncate: bool = False,
) -> bool:
    logger = get_run_logger()
    logger.info(f"Iniciando bulk_load en tabla '{table}'. Truncate={truncate}.")
    start_time = time.time()

    with db_cursor(autocommit=True) as cursor:
        if truncate:
            cursor.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE;")
            cursor.execute(f"ALTER TABLE {table} DISABLE TRIGGER ALL;")
            logger.info(f"Tabla '{table}' truncada y triggers deshabilitados.")

        # Crear CSV temporal
        count = 0
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".csv", encoding="utf-8", newline=""
        ) as tmp:
            writer = csv.writer(tmp, lineterminator="\n")
            writer.writerow(columns)
            for rec in records:
                if filter_fn and not filter_fn(rec):
                    continue
                row = []
                for col in columns:
                    val = rec.get(field_map.get(col, col)) if field_map else rec.get(col)
                    row.append(r'\N' if val in (None, "", "None") else val)
                    count += 1
                writer.writerow(row)
            tmp_path = tmp.name

        logger.info(f"{count} registros escritos en temporal: {tmp_path}")

        with open(tmp_path, "r", encoding="utf-8") as f:
            cols_sql = ", ".join(columns)
            copy_sql = (
                f"COPY {table} ({cols_sql}) "
                "FROM STDIN WITH (FORMAT csv, HEADER TRUE, NULL '\\N');"
            )
            cursor.copy_expert(copy_sql, f)
            logger.info(f"COPY ejecutado en tabla '{table}'.")

        if truncate:
            cursor.execute(f"ALTER TABLE {table} ENABLE TRIGGER ALL;")
            logger.info(f"Triggers reactivados en tabla '{table}'.")

    os.remove(tmp_path)
    elapsed = round(time.time() - start_time, 2)
    logger.info(f"bulk_load completado: {count} registros en {elapsed}s.")
    return True


# -------------------------
# Tarea para ejecutar query
# -------------------------
@task(retries=2, retry_delay_seconds=10)
def execute_query(sql_query: str):
    logger = get_run_logger()
    logger.info("Ejecutando consulta SQL...")
    with db_cursor(autocommit=True) as cursor:
        cursor.execute(sql_query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        logger.info(f"Consulta ejecutada: {len(data)} filas obtenidas.")
    return data, columns


# --------------------------------------------------
# Tarea para transformar y cargar datos desde query
# --------------------------------------------------
@task(retries=3, retry_delay_seconds=20, log_prints=False)
def load_data_table(
    name_table_target: str,
    sql_query: str,
    field_map: Optional[Dict[str, str]] = None,
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    truncate: bool = True
) -> bool:
    logger = get_run_logger()
    logger.info(f"Iniciando transformación y carga en tabla destino: {name_table_target}")
    start_time = time.time()

    data, columns = execute_query.with_options(name=f"TRANSFORMA-{name_table_target}")(sql_query)

    with db_cursor(autocommit=True) as cursor:
        if truncate:
            cursor.execute(f"TRUNCATE TABLE {name_table_target} RESTART IDENTITY CASCADE;")
            cursor.execute(f"ALTER TABLE {name_table_target} DISABLE TRIGGER ALL;")
            logger.info(f"Triggers deshabilitados en '{name_table_target}'.")

        count = 0
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".csv", encoding="utf-8", newline=""
        ) as tmp:
            writer = csv.writer(tmp, lineterminator="\n")
            writer.writerow(columns)
            for row in data:
                record = dict(zip(columns, row)) if not isinstance(row, dict) else row
                if filter_fn and not filter_fn(record):
                    continue
                out_row = [r'\N' if record.get(field_map.get(col, col) if field_map else col) in (None, "", "None")
                           else record.get(field_map.get(col, col) if field_map else col)
                           for col in columns]
                writer.writerow(out_row)
                count += 1
            tmp_path = tmp.name

        logger.info(f"{count} registros escritos en temporal: {tmp_path}")

        with open(tmp_path, "r", encoding="utf-8") as f:
            cols_sql = ", ".join(columns)
            copy_sql = (
                f"COPY {name_table_target} ({cols_sql}) "
                "FROM STDIN WITH (FORMAT csv, HEADER TRUE, NULL '\\N');"
            )
            cursor.copy_expert(copy_sql, f)
            logger.info(f"COPY ejecutado en tabla '{name_table_target}'.")

        if truncate:
            cursor.execute(f"ALTER TABLE {name_table_target} ENABLE TRIGGER ALL;")
            logger.info(f"Triggers reactivados en tabla '{name_table_target}'.")

    os.remove(tmp_path)
    elapsed = round(time.time() - start_time, 2)
    logger.info(f"load_data_table completado: {count} registros en {elapsed}s.")
    return True


# ------------------------
# Definición de los flows
# ------------------------
@flow(name="ETL-SIG:Extraccion")
def extract() -> None:
    tables = load_table_configs()
    for cfg in tables:
        data = read_dbf.with_options(name=f"LECTURA-DBF-{cfg.key.upper()}")(cfg.path)
        filter_fn = build_filter(cfg.filter_fields) if cfg.filter_fields else None
        bulk_load.with_options(name=f"CARGA-INFO-{cfg.key.upper()}")(
            table=cfg.table,
            columns=cfg.columns,
            records=data,
            field_map=cfg.field_map,
            filter_fn=filter_fn,
            truncate=cfg.truncate,
        )


@flow(name="ETL-SIG:Transformacion_carga")
def transform_load() -> None:
    query_tables = load_query_tables()
    for table in query_tables:
        load_data_table.with_options(name=f"CARGA-QUERY-{table.table}")(
            name_table_target=table.table,
            sql_query=table.query,
        )


@flow(name="ETL-SIG")
def etl_sig() -> None:
    logger = get_run_logger()
    logger.info(f"La ruta actual es: {os.getcwd()}")
    extract()
    # transform_load()
    logger.info("ETL finalizado.")


if __name__ == "__main__":
    etl_sig.serve(name="ETL-SIG-DEPLOYMENT", cron="0 0 * * *")

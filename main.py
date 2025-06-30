import io
import os
import csv
import shutil
import tempfile
import time
from typing import Any, Callable, Dict, Iterable, List, Optional
from contextlib import contextmanager
from config.utils.load_tables_config import load_table_configs, load_query_tables
from config.utils.filter import build_filter
from dbfread2 import DBF
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from config.settings_config import settings
from tasks import cargar_datos_desde_query
import psycopg2
import psycopg2.extras
# import smbclient
# import rarfile


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


# --------------------------------
# Tarea para copiar archivos SMB
# --------------------------------
# @task()
# def copy_file_from_share():
#     logger = get_run_logger()
#     logger.info("Iniciando copia de archivo desde recurso compartido")
#     try:
#         servidor = fr"{settings.quipushare}"
#         recurso = settings.recurso
#         archivo_nombre = settings.file_name
#         ruta_remota = fr"\\{servidor}\{recurso}\{archivo_nombre}"
#         ruta_local = os.path.join(settings.path_local, archivo_nombre)

#         smbclient.register_session(
#             servidor,
#             username=settings.share_username,
#             password=settings.share_password,
#         )
#         with smbclient.open_file(ruta_remota, mode='rb') as archivo_remoto:
#             with open(ruta_local, mode='wb') as archivo_local:
#                 shutil.copyfileobj(archivo_remoto, archivo_local)

#         logger.info(f"Archivo copiado a: {ruta_local}")
#     except Exception as e:
#         logger.error(f"Error al copiar el archivo: {e}")
#     return True


# ----------------------------
# Tarea para descomprimir RAR
# ----------------------------
# @task(name="DESCOMPRIMIR-RAR")
# def descomprime_rar():
#     logger = get_run_logger()
#     logger.info("Iniciando descompresion de archivo RAR")
#     rarfile.UNRAR_TOOL = "UnRAR"
#     try:
#         ruta = settings.path_local
#         if os.path.exists(ruta):
#             with rarfile.RarFile(os.path.join(ruta, "DATA.rar")) as rf:
#                 rf.extractall(path=settings.path_extract)
#                 logger.info("Archivo descomprimido correctamente.")
#         else:
#             logger.error("No se encontro la ruta especificada.")
#     except Exception as e:
#         logger.error(f"Error al descomprimir el archivo: {e}")
#     return True


# -------------------------------
# Tarea para leer archivos DBF
# -------------------------------
@task(retries=2, retry_delay_seconds=30)
def leer_dbf(path: str) -> Iterable[Dict[str, Any]]:
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
@task(retries=3, retry_delay_seconds=10, log_prints=False)
def carga_masiva_desde_dbf(
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

    with conectar_bd(autocommit=False) as (conn,cursor):
        try:
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

            if truncate:
                cursor.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE;")
                cursor.execute(f"ALTER TABLE {table} DISABLE TRIGGER ALL;")
                logger.info(f"Tabla '{table}' truncada y triggers deshabilitados.")

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
            conn.commit()
        except (Exception) as e:
            logger.error(f"Error durante la transacción para '{table}'. Revirtiendo cambios (rollback)...")
            conn.rollback()
            raise e
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
            elapsed = round(time.time() - start_time, 2)
            logger.info(f"bulk_load completado: {count} registros en {elapsed}s.")
            return True





# ------------------------
# Definición de los flows
# ------------------------
@flow(name="ETL-SIG:Extraccion")
def extraccion() -> None:
    config= load_table_configs()
    for cfg in config.tables:
        data = leer_dbf.with_options(name=f"LECTURA-DBF-{cfg.key.upper()}")(cfg.source)
        filter_fn = build_filter(cfg.filters) if cfg.filters else None
        truncate = config.defaults.get("truncate", True)
        carga_masiva_desde_dbf.with_options(name=f"CARGA-INFO-{cfg.key.upper()}")(
            table=cfg.target,
            columns=cfg.columns,
            records=data,
            field_map=cfg.field_map,
            filter_fn=filter_fn,
            truncate=truncate,
        )


@flow(name="ETL-SIG:Transformacion_carga")
def trasnformacion_carga() -> None:
    query_tables = load_query_tables()
    for table in query_tables:
        cargar_datos_desde_query.with_options(name=f"CARGA-QUERY-{table.table}")(
            name_table_target=table.table,
            sql_query=table.query,
        )


@flow(name="ETL-SIG")
def etl_sig() -> None:
    logger = get_run_logger()
    logger.info(f"La ruta actual es: {os.getcwd()}")
    extraccion()
    #transform_load()
    logger.info("ETL finalizado.")


if __name__ == "__main__":
    etl_sig.serve(name="ETL-SIG-DEPLOYMENT", cron="0 0 * * *")

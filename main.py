import io
import os
import csv
import shutil
import tempfile
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
# import smbclient
# #import aspose.zip as az 
# import rarfile

#Tarea para copiar archivos desde un recurso compartido
@task()
def copy_file_from_share():
    logger = get_run_logger()
    logger.info("Iniciando copia de archivo desde recurso compartido")
    try:
        logger.info("Inicializando configuración de SMB")
        servidor = fr"{settings.quipushare}"
        recurso = settings.recurso
        archivo_nombre = settings.file_name
        ruta_remota = fr"\\{servidor}\{recurso}\{archivo_nombre}"
        ruta_local = os.path.join(settings.path_local, archivo_nombre)
        logger.info("Termino configuracion de SMB")

        # Registrar la sesión SMB (con usuario y contraseña)
        logger.info("Abriendo sesion en el recurso compartido")
        smbclient.register_session(
        servidor,
        username=settings.share_username,
        password=settings.share_password,
        )
        logger.info("Sesion abierta correctamente")
        # Copiar archivo remoto al disco local
        
        with smbclient.open_file(ruta_remota, mode='rb') as archivo_remoto:
            with open(ruta_local, mode='wb') as archivo_local:
                shutil.copyfileobj(archivo_remoto, archivo_local)
        
        logger.info(f"Archivo copiado a: {ruta_local}")
    except Exception as e:
        logger.error(f"Error al copiar el archivo: {e}")
    return True


@task(name="DESCOMPRIMIR-RAR")
def descomprime_rar():
    logger = get_run_logger()
    logger.info("Iniciando descompresion de archivo RAR")
    rarfile.UNRAR_TOOL = "UnRAR"
    try:
        ruta = settings.path_local
        if os.path.exists(ruta):
            # Ubicar el archivo a descomprimir 
             with rarfile.RarFile(f"{settings.path_local}/DATA.rar") as rf:
                rf.extractall(path=settings.path_extract)
                logger.info("Archivo descomprimido correctamente.")
        else:
            logger.error("No se encontro la ruta especificada.")
    except Exception as e:
        logger.error(f"Error al descomprimir el archivo: {e}")
    return True


# Tarea para leer archivos DBF sin cache\@
@task(retries=2, retry_delay_seconds=30)
def read_dbf(path: str) -> Iterable[Dict[str, Any]]:
    logger = get_run_logger()
    logger.info(f"Iniciando lectura de DBF: {path}")
    full_path = os.path.join(settings.data_dir, path)
    if not os.path.exists(full_path):
        logger.error(f"Archivo DBF no encontrado: {full_path}")
        raise FileNotFoundError(f"DBF no encontrado: {full_path}")
    logger.info(f"Leyendo DBF desde ruta: {full_path}")
    records = DBF(full_path, record_factory=dict, encoding="cp1252", char_decode_errors="replace")
    #logger.info(f"Lectura completada: {len(list(records))} registros cargados (evaluación previa).")
    return records


# Tarea para crear conexión a BD sin cache\@
@task(retries=3, retry_delay_seconds=30, name="CONEXION_BD", cache_policy=NO_CACHE)
def connect_db() -> DatabaseConnection:
    logger = get_run_logger()
    logger.info("Creando conexion a la base de datos...")
    db = DatabaseConnection()
    logger.info("Conexion a la base de datos establecida correctamente.")
    return db


# Tarea para carga masiva de datos\@
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

    # 1. Conectar
    db = DatabaseConnection()
    cursor = db.connect()

    # 2. Truncate si corresponde
    if truncate:
        cursor.execute(f"TRUNCATE {table} RESTART IDENTITY CASCADE;")
        cursor.execute(f"ALTER TABLE {table} DISABLE TRIGGER ALL;")
        logger.info(f"Tabla '{table}' truncada y triggers deshabilitados.")

    # 3. Crear CSV en archivo temporal
    count = 0
    with tempfile.NamedTemporaryFile(
        mode="w+", delete=False, suffix=".csv", encoding="utf-8", newline=""
    ) as tmp:
        writer = csv.writer(tmp, lineterminator="\n")
        # Escribir cabecera
        writer.writerow(columns)
        # Escribir filas
        for rec in records:
            if filter_fn and not filter_fn(rec):
                continue
            # Mapear campos y convertir None/"" a \N para COPY
            row = []
            for col in columns:
                val = rec.get(field_map.get(col, col)) if field_map else rec.get(col)
                if val in (None, "", "None"):
                    row.append(r'\N')
                else:
                    # val ya viene decodificado CP1252 en read_dbf
                    row.append(val)
                count += 1
            writer.writerow(row)

        tmp_path = tmp.name

    logger.info(f"{count} registros escritos en temporal: {tmp_path}")

    # 4. Ejecutar COPY desde el archivo temporal
    with open(tmp_path, "r", encoding="utf-8") as f:
        cols_sql = ", ".join(columns)
        copy_sql = (
            f"COPY {table} ({cols_sql}) "
            "FROM STDIN WITH (FORMAT csv, HEADER TRUE, NULL '\\N');"
        )
        cursor.copy_expert(copy_sql, f)
        logger.info(f"COPY ejecutado en tabla '{table}'.")

    # 5. Rehabilitar triggers si truncamos
    if truncate:
        cursor.execute(f"ALTER TABLE {table} ENABLE TRIGGER ALL;")
        logger.info(f"Triggers reactivados en tabla '{table}'.")

    # 6. Limpiar y medir tiempo
    os.remove(tmp_path)
    elapsed = round(time.time() - start_time, 2)
    logger.info(f"bulk_load completado: {count} registros en {elapsed}s.")
    return True



# Tarea para ejecutar consultas\@
@task(retries=2, retry_delay_seconds=10)
def execute_query(sql_query: str):
    logger = get_run_logger()
    logger.info("Ejecutando consulta SQL...")
    db = connect_db()
    cursor = db.connect()
    cursor.execute(sql_query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    logger.info(f"Consulta ejecutada: {len(data)} filas obtenidas.")
    return data, columns


# Tarea para transformar y cargar datos desde consulta a tabla destino\@
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

    # 1. Ejecutar query y obtener datos
    data, columns = execute_query.with_options(name=f"TRANSFORMA-{name_table_target}")(sql_query)

    # 2. Conectar
    db = connect_db()
    cursor = db.connect()

    # 3. Truncate y deshabilitar triggers si corresponde
    if truncate:
        logger.info(f"Truncando y deshabilitando triggers en {name_table_target}")
        cursor.execute(f"TRUNCATE TABLE {name_table_target} RESTART IDENTITY CASCADE;")
        cursor.execute(f"ALTER TABLE {name_table_target} DISABLE TRIGGER ALL;")

    # 4. Crear CSV en archivo temporal
    count = 0
    with tempfile.NamedTemporaryFile(
        mode="w+", delete=False, suffix=".csv", encoding="utf-8", newline=""
    ) as tmp:
        writer = csv.writer(tmp, lineterminator="\n")
        writer.writerow(columns)  # cabecera

        for row in data:
            # opcional: interpretar row como dict si viene así, o lista por posición
            record = dict(zip(columns, row)) if not isinstance(row, dict) else row
            if filter_fn and not filter_fn(record):
                continue

            # preparar valores con field_map y NULL marker '\N'
            out_row = []
            for col in columns:
                key = field_map.get(col, col) if field_map else col
                val = record.get(key)
                if val in (None, "", "None"):
                    out_row.append(r'\N')
                else:
                    out_row.append(val)
            writer.writerow(out_row)
            count += 1

        tmp_path = tmp.name

    logger.info(f"{count} registros escritos en temporal: {tmp_path}")

    # 5. Ejecutar COPY desde el archivo temporal
    with open(tmp_path, "r", encoding="utf-8") as f:
        cols_sql = ", ".join(columns)
        copy_sql = (
            f"COPY {name_table_target} ({cols_sql}) "
            "FROM STDIN WITH (FORMAT csv, HEADER TRUE, NULL '\\N');"
        )
        cursor.copy_expert(copy_sql, f)
        logger.info(f"COPY ejecutado en tabla '{name_table_target}'.")

    # 6. Reactivar triggers si truncamos
    if truncate:
        cursor.execute(f"ALTER TABLE {name_table_target} ENABLE TRIGGER ALL;")
        logger.info(f"Triggers reactivados en tabla '{name_table_target}'.")

    # 7. Limpiar y medir tiempo
    os.remove(tmp_path)
    elapsed = round(time.time() - start_time, 2)
    logger.info(f"load_data_table completado: {count} registros en {elapsed}s.")
    return True

@flow(name="ETL-SIG:Extraccion")
def extract() -> None:
    #copy_file_from_share()
    #descomprime_rar()
        # ETAPA EXTRACT
    tables = load_table_configs()
    for cfg in tables:
        read_task = read_dbf.with_options(name=f"LECTURA-DBF-{cfg.key.upper()}")
        data = read_task(cfg.path)
        filter_fn = build_filter(cfg.filter_fields) if cfg.filter_fields else None
        bulk_task = bulk_load.with_options(name=f"CARGA-INFO-{cfg.key.upper()}")
        bulk_task(
            table=cfg.table,
            columns=cfg.columns,
            records=data,
            field_map=cfg.field_map,
            filter_fn=filter_fn,
            truncate=cfg.truncate,
    )

@flow(name="ETL-SIG:Transformacion_carga")
def transform_load():

    # ETAPA TRANSFORM & LOAD
    query_tables = load_query_tables()
    for table in query_tables:
        load_task = load_data_table.with_options(name=f"CARGA-QUERY-{table.table}")
        load_task(table.table, table.query)


@flow(name="ETL-SIG")
def etl_sig() -> None:
    logger = get_run_logger()
    ruta_actual = os.getcwd()
    logger.info(f"La ruta actual es: {ruta_actual}")
    extract()
    #transform_load()
    db =  connect_db()
    db.close()


if __name__ == "__main__":
    etl_sig.serve(name="ETL-SIG-DEPLOYMENT",cron="0 0 * * *")
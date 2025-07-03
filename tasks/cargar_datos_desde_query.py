# --------------------------------------------------
# Tarea para transformar y cargar datos desde query
# --------------------------------------------------
import csv
import os
import time
from typing import Any, Callable, Dict, Optional
import tempfile
from prefect import get_run_logger, task
from context.conectar_db import conectar_bd



@task(retries=3, retry_delay_seconds=20)
def cargar_datos_desde_query(
    anio:str,
    name_table_target: str,
    sql_query: str,
    field_map: Optional[Dict[str, str]] = None,
    filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    truncate: bool = True
) -> bool:
    logger = get_run_logger()
    logger.info(f"Iniciando transformacion y carga en tabla destino: {name_table_target}")
    start_time = time.time()


    with conectar_bd(autocommit=False) as (conn,cursor):
        try:
            logger.info("Ejecutando consulta SQL...")
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            count = 0
            with tempfile.NamedTemporaryFile(
                mode="w+", delete=False, suffix=".csv", encoding="utf-8", newline=""
            ) as tmp:
                writer = csv.writer(tmp, lineterminator="\n")
                writer.writerow(columns)
                for row in cursor:
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

            if truncate:
                cursor.execute(f"TRUNCATE TABLE {name_table_target}_{anio} RESTART IDENTITY CASCADE;")
                cursor.execute(f"ALTER TABLE {name_table_target}_{anio} DISABLE TRIGGER ALL;")
                logger.info(f"Triggers deshabilitados en '{name_table_target}_{anio}'.")


            with open(tmp_path, "r", encoding="utf-8") as f:
                cols_sql = ", ".join(columns)
                copy_sql = (
                    f"COPY {name_table_target}_{anio} ({cols_sql}) "
                    "FROM STDIN WITH (FORMAT csv, HEADER TRUE, NULL '\\N');"
                )
                cursor.copy_expert(copy_sql, f)
                logger.info(f"COPY ejecutado en tabla '{name_table_target}'.")

            if truncate:
                cursor.execute(f"ALTER TABLE {name_table_target}_{anio} ENABLE TRIGGER ALL;")
                logger.info(f"Triggers reactivados en tabla '{name_table_target}_{anio}'.")
            conn.commit()
        except (Exception) as e:
            logger.error(f"Error durante la transaccion para transformacion y carga '{name_table_target}'. Revirtiendo cambios (rollback)...")
            logger.error(e)
            conn.rollback()
            raise e
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
            elapsed = round(time.time() - start_time, 2)
            logger.info(f"load_data_table completado: {count} registros en {elapsed}s.")
            return True
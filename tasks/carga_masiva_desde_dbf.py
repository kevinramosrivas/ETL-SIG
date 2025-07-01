import csv
import os
import tempfile
import time
from typing import Any, Callable, Dict, Iterable, List, Optional
from prefect import get_run_logger, task
from context.conectar_db import conectar_bd

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
            logger.error(f"Error durante la transaccion para '{table}'. Revirtiendo cambios (rollback)...")
            conn.rollback()
            raise e
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
            elapsed = round(time.time() - start_time, 2)
            logger.info(f"bulk_load completado: {count} registros en {elapsed}s.")
            return True
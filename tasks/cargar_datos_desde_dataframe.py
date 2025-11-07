import csv
import os
import time
import tempfile
from typing import Optional, Callable, List, Any

import pandas as pd
from prefect import get_run_logger, task
from context.conectar_db import conectar_bd

@task(retries=3, retry_delay_seconds=10, log_prints=False)
def cargar_datos_desde_dataframe(
    anio: str,
    table: str,
    df: pd.DataFrame,
    truncate: bool = True,
    particionada: bool = True
) -> bool:
    """
    Carga un DataFrame en PostgreSQL mediante COPY usando un archivo temporal.

    Args:
        anio: año de la partición.
        table: nombre base de la tabla destino.
        df: pandas.DataFrame con los datos a cargar.
        truncate: si True, hace TRUNCATE previo (y deshabilita triggers).
        particionada: si True, agrega el sufijo del año a la tabla.
    """
    logger = get_run_logger()
    full_table_name = f"{table}_{anio}" if particionada else table
    logger.info(f"Iniciando bulk_load en tabla '{full_table_name}'. Truncate={truncate}")
    start_time = time.time()
    count = 0
    tmp_path = None

    # Obtener columnas
    columns: List[str] = list(df.columns)

    with conectar_bd(autocommit=False) as (conn, cur):
        try:
            # 1) Escribir CSV a archivo temporal
            with tempfile.NamedTemporaryFile(
                mode="w+", delete=False, suffix=".csv", encoding="utf-8", newline=""
            ) as tmp:
                writer = csv.writer(tmp, lineterminator="\n")
                writer.writerow(columns)
                for _, row in df.iterrows():
                    record = [
                        r'\N' if pd.isna(val) or val == "" else val
                        for val in row.tolist()
                    ]
                    writer.writerow(record)
                    count += 1
                tmp_path = tmp.name
            logger.info(f"  → {count} registros escritos en temporal: {tmp_path}")

            # 2) Truncate y deshabilitar triggers si corresponde
            if truncate:
                cur.execute(f"TRUNCATE TABLE {full_table_name} RESTART IDENTITY CASCADE;")
                cur.execute(f"ALTER TABLE {full_table_name} DISABLE TRIGGER ALL;")
                logger.info(f"Tabla '{full_table_name}' truncada y triggers deshabilitados")

            # 3) COPY desde el archivo temporal
            with open(tmp_path, "r", encoding="utf-8") as f:
                cols_sql = ", ".join(columns)
                copy_sql = (
                    f"COPY {full_table_name} ({cols_sql}) "
                    "FROM STDIN WITH (FORMAT csv, HEADER TRUE, NULL '\\N');"
                )
                cur.copy_expert(copy_sql, f)
            logger.info(f"COPY completado en tabla '{full_table_name}'")

            # 4) Rehabilitar triggers si se truncó
            if truncate:
                cur.execute(f"ALTER TABLE {full_table_name} ENABLE TRIGGER ALL;")
                logger.info(f"Triggers reactivados en tabla '{full_table_name}'")

            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception(f"Error durante la carga en '{full_table_name}', rollback aplicado")
            raise
        finally:
            # 5) Eliminar el archivo temporal
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
            cur.close()
            conn.close()
            elapsed = time.time() - start_time
            logger.info(f"bulk_load completado: {count} registros en {elapsed:.2f}s")

    return True

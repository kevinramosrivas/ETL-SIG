from prefect import get_run_logger, task
from context.conectar_db import conectar_bd

@task(retries=1, retry_delay_seconds=10)
def crear_particiones(nombre_tabla: str, anio: str) -> list[str]:
    logger = get_run_logger()
    with conectar_bd(autocommit=True) as (conn,cursor):
        sql = f"""CREATE IF NO EXIST TABLE {nombre_tabla}_{anio}
            PARTITION OF {nombre_tabla}
            FOR VALUES IN ({anio});"""
        logger.info(sql)
        # cursor.execute(sql)

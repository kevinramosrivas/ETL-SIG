from prefect import task, get_run_logger
from psycopg2 import sql
from context.conectar_db import conectar_bd

@task(retries=1, retry_delay_seconds=10)
def actualizar_vw_materializadas(nombre_tabla: str, esquema:str) -> None:
    logger = get_run_logger()

    # SQL para actualizar vista
    update_query = sql.SQL("REFRESH MATERIALIZED VIEW CONCURRENTLY {}.{}").format(
        sql.Identifier(esquema),
        sql.Identifier(nombre_tabla)
    )

    with conectar_bd(autocommit=True) as (conn, cursor):
        try:
            logger.info(f"Actualizando vista materializada: {esquema}.{nombre_tabla}")
            cursor.execute(update_query)
            logger.info(f"Vista actualizada correctamente:'{esquema}.{nombre_tabla}'")
        except Exception as e:
            logger.error(f"Error al actualizar la vista: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()

from prefect import task, get_run_logger
from psycopg2 import sql
from context.conectar_db import conectar_bd

@task(retries=1, retry_delay_seconds=10)
def crear_particiones(nombre_tabla: str, anio: str) -> None:
    logger = get_run_logger()

    if not anio.isdigit():
        raise ValueError(f"Año inválido: {anio}")

    nombre_particion = f"{nombre_tabla}_{anio}"

    check_sql = sql.SQL("""
        SELECT to_regclass({partition});
    """).format(
        partition=sql.Literal(nombre_particion)
    )

    create_sql = sql.SQL("""
        CREATE TABLE {partition}
        PARTITION OF {parent}
        FOR VALUES IN (%s);
    """).format(
        partition=sql.Identifier(nombre_particion),
        parent=sql.Identifier(nombre_tabla)
    )

    with conectar_bd(autocommit=True) as (conn, cursor):
        logger.info(f"Verificando existencia de partición: {nombre_particion}")
        cursor.execute(check_sql)
        exists = cursor.fetchone()[0] is not None

        if exists:
            logger.info(f"La partición '{nombre_particion}' ya existe. No se crea nuevamente.")
            return

        logger.info(f"Creando partición '{nombre_particion}' para el año {anio}")
        cursor.execute(create_sql, [anio])
        logger.info(f"Partición '{nombre_particion}' creada correctamente.")

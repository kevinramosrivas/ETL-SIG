from prefect import task, get_run_logger
from psycopg2 import sql
from context.conectar_db import conectar_bd

@task(retries=1, retry_delay_seconds=10)
def crear_particiones(nombre_tabla: str, anio: str) -> None:
    logger = get_run_logger()

    # Validaciones básicas
    if not anio.isdigit():
        raise ValueError(f"Ano invalido: {anio}")
    if "." not in nombre_tabla:
        raise ValueError(f"El nombre de la tabla debe incluir el esquema: {nombre_tabla}")

    esquema, tabla = nombre_tabla.split(".")
    nombre_particion = f"{tabla}_{anio}"

    # SQL para verificar existencia
    check_sql = sql.SQL("""
        SELECT to_regclass({partition});
    """).format(
        partition=sql.Literal(f"{esquema}.{nombre_particion}")
    )

    # SQL para crear partición
    create_sql = sql.SQL("""
        CREATE TABLE {esquema}.{partition}
        PARTITION OF {esquema}.{parent}
        FOR VALUES IN (%s);
    """).format(
        esquema=sql.Identifier(esquema),
        partition=sql.Identifier(nombre_particion),
        parent=sql.Identifier(tabla)
    )

    with conectar_bd(autocommit=True) as (conn, cursor):
        try:
            logger.info(f"Verificando existencia de particion: {esquema}.{nombre_particion}")
            cursor.execute(check_sql)
            exists = cursor.fetchone()[0] is not None

            if exists:
                logger.info(f"La particion '{esquema}.{nombre_particion}' ya existe. No se crea nuevamente.")
                return

            logger.info(f"Creando particion '{esquema}.{nombre_particion}' para el ano {anio}")
            cursor.execute(create_sql, [anio])
            logger.info(f"Particion '{esquema}.{nombre_particion}' creada correctamente.")
        except Exception as e:
            logger.error(f"Error al crear la particion: {e}")
            raise

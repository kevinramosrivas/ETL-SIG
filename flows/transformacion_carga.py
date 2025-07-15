from prefect import flow, get_run_logger
from tasks.crear_particiones import crear_particiones
from tasks.cargar_datos_desde_query import cargar_datos_desde_query
from config.utils.load_tables_config import get_years_to_extract, load_table_tranform

@flow(name="ETL-SIG:Transformacion_carga")
def transformacion_carga() -> None:
    logger = get_run_logger()
    cargadas_no_particionadas = set()

    for anio in get_years_to_extract():
        logger.info(f"Iniciando anualidad: {anio}")
        config = load_table_tranform(anio)

        for table_cfg in config.tables:
            tabla = table_cfg.table

            if table_cfg.partitioned:
                # Ejecutar creación de partición y esperar antes de seguir con la carga
                crear_particiones \
                    .with_options(name=f"CREAR-PARTICION-{tabla}-{anio}") \
                    .submit(nombre_tabla=tabla, anio=anio) \
                    .result()  # bloquea hasta que termine

                # Luego hacer la carga
                cargar_datos_desde_query \
                    .with_options(name=f"CARGA-PARTICION-{tabla}-{anio}") \
                    .submit(
                        anio=anio,
                        name_table_target=tabla,
                        sql_query=table_cfg.query,
                        particionada=True,
                    ) \
                    .result()  # espera también la carga, útil para detectar fallos

            else:
                if tabla not in cargadas_no_particionadas:
                    logger.info(f"Cargando unica vez (no particionada): {tabla}")
                    cargar_datos_desde_query \
                        .with_options(name=f"CARGA-TABLA-{tabla}") \
                        .submit(
                            anio=anio,
                            name_table_target=tabla,
                            sql_query=table_cfg.query,
                            particionada=False,
                        ) \
                        .result()
                    cargadas_no_particionadas.add(tabla)

    logger.info("Todas las tareas completaron correctamente.")

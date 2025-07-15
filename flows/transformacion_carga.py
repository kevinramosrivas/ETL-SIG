from prefect import flow, get_run_logger
from prefect.futures import wait
from tasks.crear_particiones import crear_particiones
from tasks.cargar_datos_desde_query import cargar_datos_desde_query
from config.utils.load_tables_config import get_years_to_extract, load_table_tranform

@flow(name="ETL-SIG:Transformacion_carga")
def transformacion_carga() -> None:
    logger = get_run_logger()
    cargadas_no_particionadas = set()
    futures = []

    for anio in get_years_to_extract():
        logger.info(f"Iniciando anualidad: {anio}")
        config = load_table_tranform(anio)

        for table_cfg in config.tables:
            tabla = table_cfg.table

            if table_cfg.partitioned:
                # 1) Crear partición con nombre personalizado
                futures.append(
                    crear_particiones
                    .with_options(name=f"CREAR-PARTICION-{tabla}-{anio}")
                    .submit(nombre_tabla=tabla, anio=anio)
                )

                # 2) Cargar datos en la partición
                futures.append(
                    cargar_datos_desde_query
                    .with_options(name=f"CARGA-PARTICION-{tabla}-{anio}")
                    .submit(
                        anio=anio,
                        name_table_target=tabla,
                        sql_query=table_cfg.query,
                        particionada=True,
                    )
                )
            else:
                if tabla not in cargadas_no_particionadas:
                    # Carga única para tablas no particionadas
                    futures.append(
                        cargar_datos_desde_query
                        .with_options(name=f"CARGA-TABLA-{tabla}")
                        .submit(
                            anio=anio,  # o cualquier año de referencia
                            name_table_target=tabla,
                            sql_query=table_cfg.query,
                            particionada=False,
                        )
                    )
                    cargadas_no_particionadas.add(tabla)

    # Esperamos a que terminen todas; si alguna falla, abortamos el flow
    wait(futures, raise_on_exception=True)
    logger.info("✅ Todas las tareas completaron correctamente.")

from importlib import import_module
from prefect import flow, get_run_logger
from tasks.crear_particiones import crear_particiones
from tasks.cargar_datos_desde_query import cargar_datos_desde_query
from tasks.cargar_datos_desde_dataframe import cargar_datos_desde_dataframe
from config.utils.load_tables_config import get_years_to_extract, load_table_tranform

@flow(name="ETL-SIG:Transformacion_carga")
def transformacion_carga() -> None:
    logger = get_run_logger()
    cargadas_no_particionadas = set()

    for anio in get_years_to_extract():
        logger.info(f"Iniciando anualidad: {anio}")
        config = load_table_tranform(anio)

        for tcfg in config.tables:
            tabla = tcfg.table
            if tcfg.source_type == "sql":
                # --- RUTINA SQL (igual que antes) ---
                if tcfg.partitioned:
                    # Crear partición
                    crear = crear_particiones \
                        .with_options(name=f"CREAR-PARTICION-{tabla}-{anio}") \
                        .submit(nombre_tabla=tabla, anio=anio)
                    # Cargar con dependencia
                    carga = cargar_datos_desde_query \
                        .with_options(name=f"CARGA-PARTICION-{tabla}-{anio}") \
                        .submit(
                            anio=anio,
                            name_table_target=tabla,
                            sql_query=tcfg.query,
                            particionada=True,
                            wait_for=[crear],
                        )
                    carga.result()
                else:
                    if tabla not in cargadas_no_particionadas:
                        carga = cargar_datos_desde_query \
                            .with_options(name=f"CARGA-TABLA-{tabla}") \
                            .submit(
                                anio=anio,
                                name_table_target=tabla,
                                sql_query=tcfg.query,
                                particionada=False,
                            )
                        carga.result()
                        cargadas_no_particionadas.add(tabla)

            if tcfg.source_type == "scraper":
                # --- RUTINA WEB‑SCRAPING ---
                # 1) Dinámicamente importa la tarea de scraping
                scraper_mod = import_module(tcfg.scraper.module)
                scrape_fn   = getattr(scraper_mod, tcfg.scraper.function)
                # Crear partición
                crear = crear_particiones \
                        .with_options(name=f"CREAR-PARTICION-{tabla}-{anio}") \
                        .submit(nombre_tabla=tabla, anio=anio)
                # 2) Ejecuta y espera el DataFrame
                df = scrape_fn \
                    .with_options(name=f"SCRAPE-{tabla}") \
                    .submit(year = anio, 
                            target_table = tabla,
                            wait_for=[crear] 
                    )
                cargar_datos_desde_dataframe\
                    .with_options(name=f"CARGA-TABLA-{tabla}")\
                    .submit(
                        anio = anio,
                        table = tabla,
                        df = df,
                        wait_for=[df]
                    ).result()
            else:
                logger.error(f"Tipo de origen desconocido para {tabla}: {tcfg.source_type}")

    logger.info("✅ Todas las tareas completaron correctamente.")

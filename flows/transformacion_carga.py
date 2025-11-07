from importlib import import_module
from prefect import flow, get_run_logger
from tasks.crear_particiones import crear_particiones
from tasks.cargar_datos_desde_query import cargar_datos_desde_query
from tasks.cargar_datos_desde_dataframe import cargar_datos_desde_dataframe
from tasks.actualizar_vw_materializadas import actualizar_vw_materializadas
from config.utils.load_tables_config import get_years_to_extract, load_table_tranform
from config.utils.load_views_config import load_views_configs
from config.env_config import settings


def _procesar_tabla(
    logger,
    anio: str,
    tcfg,
    cargadas_no_particionadas: set[str],
):
    tabla = tcfg.table
    logger.info("------------------------------------------------------------")
    logger.info(f"Procesando tabla: {tabla} ({tcfg.source_type})")
    logger.info("------------------------------------------------------------")
   

    # Si la tabla es particionada, creamos primero la partición
    crear = None
    if tcfg.partitioned:
        crear = crear_particiones \
            .with_options(name=f"CREAR-PARTICION-{tabla}-{anio}") \
            .submit(nombre_tabla=tabla, anio=anio)

    if tcfg.source_type == "sql":
        # Carga SQL
        if tcfg.partitioned:
            cargar_datos_desde_query \
                .with_options(name=f"CARGA-PART-{tabla}-{anio}") \
                .submit(
                    anio=anio,
                    name_table_target=tabla,
                    sql_query=tcfg.query,
                    particionada=True,
                    wait_for=[crear],
                ).result()
        else:
            if tabla not in cargadas_no_particionadas:
                cargar_datos_desde_query \
                    .with_options(name=f"CARGA-TABLA-{tabla}") \
                    .submit(
                        anio=anio,
                        name_table_target=tabla,
                        sql_query=tcfg.query,
                        particionada=False,
                    ).result()
                cargadas_no_particionadas.add(tabla)

    elif tcfg.source_type == "scraper":
        # Web-scraping dinámico
        mod = import_module(tcfg.scraper.module)
        fn  = getattr(mod, tcfg.scraper.function)

        df = fn \
            .with_options(name=f"SCRAPE-{tabla}") \
            .submit(year=anio, target_table=tabla, wait_for=[crear])

        cargar_datos_desde_dataframe \
            .with_options(name=f"CARGA-DATAFRAME-{tabla}") \
            .submit(anio=anio, table=tabla, df=df, wait_for=[df]) \
            .result()

    else:
        logger.warning(f"Tipo no soportado: {tcfg.source_type} para {tabla}")

    logger.info("------------------------------------------------------------")


@flow(name="ETL-SIG:Transformacion_carga")
def transformacion_carga() -> None:
    YEARS_TO_EXTRACT = settings.sig_extraccion_anios_historicos
    logger = get_run_logger()
    cargadas_no_particionadas: set[str] = set()

    logger.info("=== Inicio: Procesamiento de Tablas DIMENSION_BASE ===")
    for anio in get_years_to_extract(YEARS_TO_EXTRACT):
        logger.info(f"Anio {anio} (DIMENSION_BASE)")
        config_fact = load_table_tranform(anio, table_type="dimension_base")
        for tcfg in config_fact.tables:
            _procesar_tabla(logger, anio, tcfg, cargadas_no_particionadas)


    #Primero: todas las tablas FACT, año por año
    logger.info("=== Inicio: Procesamiento de Tablas FACT ===")
    for anio in get_years_to_extract(YEARS_TO_EXTRACT):
        logger.info(f"Anio {anio} (FACT)")
        config_fact = load_table_tranform(anio, table_type="fact")
        for tcfg in config_fact.tables:
            _procesar_tabla(logger, anio, tcfg, cargadas_no_particionadas)

    #Después: todas las tablas DIMENSION, año por año
    logger.info("=== Inicio: Procesamiento de Tablas DIMENSION ===")
    for anio in get_years_to_extract(YEARS_TO_EXTRACT):
        logger.info(f"Anio {anio} (DIMENSION)")
        config_dim = load_table_tranform(anio, table_type="dimension")
        for tcfg in config_dim.tables:
            _procesar_tabla(logger, anio, tcfg, cargadas_no_particionadas)

    config_vw_m = load_views_configs()
    for vwm in config_vw_m.tables:
        logger.info(f"Vista {vwm}")
        actualizar_vw_materializadas \
            .with_options(name=f"ACTUALIZAR-VISTA-{vwm}") \
            .submit(nombre_tabla=vwm,esquema=config_vw_m.schema_name) \
            .result()
    logger.info("Todas las tareas completaron correctamente")

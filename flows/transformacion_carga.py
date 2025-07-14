from prefect import flow, get_run_logger
from tasks.crear_particiones import crear_particiones
from tasks.cargar_datos_desde_query import cargar_datos_desde_query
from config.utils.load_tables_config import get_years_to_extract, load_table_tranform

@flow(name="ETL-SIG:Transformacion_carga")
def transformacion_carga() -> None:
    logger = get_run_logger()
    # Llevar un registro de tablas no particionadas ya cargadas
    cargadas_no_particionadas = set()

    for anio in get_years_to_extract():
        logger.info(f"Iniciando anualidad: {anio}")
        config = load_table_tranform(anio)

        for table_cfg in config.tables:
            tabla = table_cfg.table
            # --- TABLAS PARTICIONADAS: una ejecución por año ---
            if table_cfg.partitioned:
                crear_particiones.with_options(
                    name=f"CREAR-PART_{tabla}_{anio}"
                )(nombre_tabla=tabla, anio=anio)

                cargar_datos_desde_query.with_options(
                    name=f"CARGA-PART_{tabla}_{anio}"
                )(
                    anio=anio,
                    name_table_target=tabla,
                    sql_query=table_cfg.query,
                    particionada=True,
                )

            # --- TABLAS NO PARTICIONADAS: solo UNA ejecución global ---
            else:
                if tabla not in cargadas_no_particionadas:
                    logger.info(f"Cargando unica vez (no particionada): {tabla}")
                    cargar_datos_desde_query.with_options(
                        name=f"CARGA-NOPART_{tabla}"
                    )(
                        anio=anio,  # puedes pasar un valor dummy o el primer año
                        name_table_target=tabla,
                        sql_query=table_cfg.query,
                        particionada=False,
                    )
                    cargadas_no_particionadas.add(tabla)

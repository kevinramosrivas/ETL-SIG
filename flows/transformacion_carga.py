from prefect import flow, get_run_logger
from tasks.cargar_datos_desde_query import cargar_datos_desde_query
from config.utils.load_tables_config import load_query_tables, load_table_tranform


@flow(name="ETL-SIG:Transformacion_carga")
def trasnformacion_carga() -> None:
    logger = get_run_logger()
    config_table_tranform = load_table_tranform("2025")
    logger.info(f"{config_table_tranform}")
    # query_tables = load_query_tables(anio)
    # for table in query_tables:

    #     cargar_datos_desde_query.with_options(name=f"CARGA-QUERY-{table.table}")(
    #         name_table_target=table.table,
    #         sql_query=table.query,
    #     )
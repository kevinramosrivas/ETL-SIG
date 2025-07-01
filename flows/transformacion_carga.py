from prefect import flow
from tasks.cargar_datos_desde_query import cargar_datos_desde_query
from config.utils.load_tables_config import load_query_tables


@flow(name="ETL-SIG:Transformacion_carga")
def trasnformacion_carga() -> None:
    query_tables = load_query_tables()
    for table in query_tables:
        cargar_datos_desde_query.with_options(name=f"CARGA-QUERY-{table.table}")(
            name_table_target=table.table,
            sql_query=table.query,
        )
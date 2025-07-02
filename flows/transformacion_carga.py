from prefect import flow
from tasks.cargar_datos_desde_query import cargar_datos_desde_query
from tasks.crear_particiones import crear_particiones
from config.utils.load_tables_config import get_years_to_extract,load_table_tranform


@flow(name="ETL-SIG:Transformacion_carga")
def trasnformacion_carga() -> None:
    for anio in get_years_to_extract():
        config_table_tranform = load_table_tranform(anio)
        for table_cfg in config_table_tranform.tables:
            if table_cfg.partitioned:
                crear_particiones(table_cfg.table,anio)
            # cargar_datos_desde_query.with_options(name=f"CARGA-QUERY-{table_cfg.table}")(
            #     name_table_target=table_cfg.table,
            #     sql_query=table_cfg.query,
            # )
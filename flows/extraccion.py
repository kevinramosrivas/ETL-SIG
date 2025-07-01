from prefect import flow
from tasks.carga_masiva_desde_dbf import carga_masiva_desde_dbf
from tasks.leer_dbf import leer_dbf
from config.utils.filter import build_filter
from config.utils.load_tables_config import load_table_configs
from prefect import get_run_logger

# ------------------------
# Definición de los flows
# ------------------------
@flow(name="ETL-SIG:Extraccion")
def extraccion() -> None:
    logger = get_run_logger()
    config= load_table_configs()
    for cfg in config.tables:
        data = leer_dbf.with_options(name=f"LECTURA-DBF-{cfg.key.upper()}")(cfg.source)
        filter_fn = build_filter(cfg.filters) if cfg.filters else None
        logger.info(f"Filtros usados en DBF{cfg.source}: {cfg.filters}")
        truncate = config.defaults.get("truncate", True)
        carga_masiva_desde_dbf.with_options(name=f"CARGA-INFO-{cfg.key.upper()}")(
            table=cfg.target,
            columns=cfg.columns,
            records=data,
            field_map=cfg.field_map,
            filter_fn=filter_fn,
            truncate=truncate,
        )
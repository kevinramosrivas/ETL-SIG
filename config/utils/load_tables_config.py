import datetime
from config.tables_config import TABLES_QUERY,TableQuery
from typing import List
from config.models.extract_model import ExtraTableSettings 
from config.models.transform_model import TransformTablesConfig
from pathlib import Path
import yaml


def get_years_to_extract(n=1):
    # Leer año actual desde variable, si no existe usar el año actual del sistema
    periodo_hasta = int(datetime.datetime.now().now().year)
    periodo_desde = int(datetime.datetime.now().now().year - n) 
    anios = []
    if(periodo_desde>= periodo_hasta):
        raise Exception("el periodo desde no puede ser mayor al periodo hasta")
    for year in range(periodo_desde, periodo_hasta + 1):
        anios.append(str(year))
    return anios


def load_table_configs(path: str = "extract_config.yaml") -> ExtraTableSettings:
    full_path = Path(__file__).parent.parent / path
    with open(full_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    years = get_years_to_extract()
    for table in raw_config.get("tables", []):
        filters = table.get("filters", {})
        for k, v in filters.items():
            if v == "__YEARS__":
                filters[k] = years

    return ExtraTableSettings(**raw_config)


def load_table_tranform(year:str ,path: str = "transform_config.yaml") -> TransformTablesConfig:
    full_path = Path(__file__).parent.parent / path
    with open(full_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)
    for table in raw_config.get("tables", []):
        query:str = table.get("query","")
        if "__ANIO_EJECUCION__" in query:
            query = query.replace("__ANIO_EJECUCION__",year)
    return TransformTablesConfig(**raw_config)
    


def load_query_tables() -> List[TableQuery]:
    configs: List[TableQuery] = []
    for info in TABLES_QUERY:
        configs.append(
            TableQuery(
                table=info["table"],
                query= info["query"],
            )
        )
    return configs

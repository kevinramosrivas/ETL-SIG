import datetime
from config.tables_config import TABLES_QUERY,TableQuery
from typing import List
from config.models.extract_model import ExtraTableSettings 
from pathlib import Path
from prefect import variables
import yaml
import json


def get_years_to_extract(n=2):
    # Leer año actual desde variable, si no existe usar el año actual del sistema
    periodo_desde = int(variables.get("periodo_desde", default=str(datetime.datetime.now().now().year)))
    periodo_hasta = int(variables.get("periodo_hasta", default=str(datetime.datetime.now().now().year - n))) 
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
